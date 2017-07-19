"""
By Diogo Nicolau

Recommendation engine based on Spark mllib ALS and Annoy
Recommends a list of items to every item in the dataset

Input:
	- number of months to extract data;
	- s3 path;
	- number of recommendations per app
	Example:
	    spark-submit --master yarn --deploy-mode client --packages org.postgresql:postgresql:9.4.1211.jre7
	    recommendALS_items2items.py 2 s3://aptoide-big-data/reco/DN/May_June 10
Output:
	- recommendation files in the following format:
		"app1",("app_reco1",score),("app_reco2",score) ...
		"app2",("app_reco1",score),("app_reco2",score) ...
	- the recommendation files are saved in the given s3 path under "recommendations"
	- in the given s3 path there will be saved aswell:
	    - "apps": mapping of internal id and app name
	    - "hashs": mapping of the internal id and user hash
	    - "dataset": files ready for model training
	    - "histogram": mapping of the app name and number of occurrences
"""

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.recommendation import ALS, Rating
from pyspark import SparkFiles
from annoy import AnnoyIndex
import sys
import csv
from prepare_data import prepare_data
import os
import boto3
import botocore


def main(months, output_path, number_recs):

    conf = SparkConf().setAppName("AptoideALS")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")

    # Connects to s3 and collects information related to the select amount of months
    ratings = get_ratings(months, output_path, sc)

    # Number of features used to describe items
    rank = 120

    model = get_model(ratings, rank)
    load_annoy_index(rank, model, sc)
    names_data = get_names_from_s3(output_path)
    categories_data = get_categories_from(sc, names_data)

    # Broadcast: improve performance by sending once per node rather than a once per task
    names = sc.broadcast(names_data)
    categories = sc.broadcast(categories_data)

    # Major function to get recommendations based on features vectors
    # assumes items are numbered 0 ... n-1
    def find_neighbors(iter):
        t = AnnoyIndex(rank)
        t.load(SparkFiles.get("index.ann"))
        return ((x[0]-1, t.get_nns_by_item(x[0]-1, int(number_recs), search_k=len(names_data))) for x in iter)

    # Function to convert into the format required and filter by category
    def construct_string(x):
        array = []
        order = int(number_recs)

        app_name = names.value[x[0]]
        app_category = categories.value[app_name]

        if app_category is None:
            for item in x[1]:
                item_name = names.value[item]
                array.append("(\"{}\",{})".format(item_name, str(order)))
                order -= 1

            result = "\"{}\",{}".format(app_name, str(array)).replace(" ", "").replace("[", "").replace("]", "").replace("'", "")
            return result
        else:
            for item in x[1]:
                item_name = names.value[item]
                item_category = categories.value[item_name]

                if app_name != item_name and app_category == item_category:
                    array.append("(\"{}\",{})".format(item_name, str(order)))
                    order -= 1

            result = "\"{}\",{}".format(app_name, str(array)).replace(" ", "").replace("[", "").replace("]", "").replace("'", "")
            return result

    similarRDD = model.productFeatures().mapPartitions(find_neighbors)
    similarRDD.map(construct_string).saveAsTextFile(output_path + "/recommendations")


def get_ratings(months, output_path, sc):
    # Load and parse the data if path doesn't exist
    client = boto3.client('s3')
    try:
        client.get_object(Bucket='bucket', Key='key')
        data = sc.textFile("s3file".format(output_path))
        return data.map(lambda l: l.split(',')) \
            .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()

    except (AttributeError, botocore.exceptions.ClientError):
        # If dataset doesn't exist in s3 path prepare it from the install logs
        data = prepare_data(sc, int(months), output_path)
        return data.map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()


def get_model(ratings, rank):
    # Build the recommendation model using Alternating Least Squares
    seed = 5L
    iterations = 12
    # Is a basic L2 Regularizer to reduce overfitting
    regularization_parameter = 0.000001
    # Number of features used to describe items
    # rank = 120
    # Is the confidence that we have that the user likes the item
    alpha = 1000.0

    print("Training Model")
    model = ALS.trainImplicit(ratings,
                              rank,
                              seed=seed,
                              iterations=iterations,
                              lambda_=regularization_parameter,
                              alpha=alpha)
    print("Model Trained")

    # Evaluate the model on training data
    # Takes some time and is not necessary
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()
    print("Mean Squared Error = " + str(MSE))
    return model


def load_annoy_index(rank, model, sc):
    # Use Spotify annoy to get items neighbors based on the feature vectors
    # n_trees -> a larger value will give more accurate results, but larger indexes
    # search_k -> a larger value will give more accurate results, but will take longer time to return
    index = AnnoyIndex(rank, 'angular')
    items = model.productFeatures().collect()
    for i, vector in items:
        # Annoy start at index 0, while Spark starts at index 1. We need to -1 every index
        index.add_item(i - 1, vector)
    # n_trees
    index.build(500)
    index.save("index.ann")
    sc.addPyFile("index.ann")


def get_names_from_s3(output_path):
    # Load the names produced by prepare_data saved on the s3 path
    os.system("aws s3 cp s3file apps".format(output_path))
    names_data = {}
    with open("apps") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            names_data[int(row[1]) - 1] = row[0]
            # indexes in annoy start at 0
    return names_data


def get_categories_from(sc, names_data):
    # TODO Categories should be always actualized, however currently it is impossible to connect with DB and extract
    # TODO the required info. Cus of this a static file is used that doesn't guarantee accurate categories.
    categories_data = {}
    try:
        # Check if file is in s3 and create dict of apps -> categories
        client = boto3.client('s3')
        categories_data_raw = {}
        client.get_object(Bucket='bucket', Key='key')
        os.system("aws s3 cp s3file categories")
        with open("categories") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                categories_data_raw[row[0]] = row[1]

        for app in names_data.values():
            try:
                categories_data[app] = categories_data_raw[app]
            except (Exception):
                categories_data[app] = None

        return categories_data

    except (AttributeError, botocore.exceptions.ClientError):
        # If category file is not present in the s3 path connect to database and get categories per app
        sqlContext = SQLContext(sc)
        u, p = ['user', 'password']
        durl = 'url'
        query1 = "(SELECT apk_id and fk_category_id FROM {} ".format('apks') + "WHERE apk_id = ANY('{}') ".format(names_data.values())
        psql_df_apks = sqlContext.read.format('jdbc').options(url=durl + "?currentSchema=apks",
                                                              user=u,
                                                              password=p,
                                                              dbtable='table',
                                                              query=query1,
                                                              driver='org.postgresql.Driver').load()

        psql_df_categories = sqlContext.read.format('jdbc').options(url=durl,
                                                                    user=u,
                                                                    password=p,
                                                                    dbtable='table',
                                                                    driver='org.postgresql.Driver').load().select('id', 'name')

        apks_categories = psql_df_apks.join(psql_df_categories, psql_df_apks['fk_category_id'] == psql_df_categories['id'])
        categories_list = apks_categories.select('apk_id', 'name').collect()
        categories_data = {}
        for items in categories_list:
            categories_data[items[0]] = items[1]

        return categories_data

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])