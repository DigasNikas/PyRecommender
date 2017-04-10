"""
By Diogo Nicolau

Recommendation engine based on Spark mllib ALS
Recommends a list of users to every item in the dataset

Input:
	- csv dataset;
	- csv id -> user_hash;
	- csv id -> app_names;
	- number of recommendations per item
Output:
	- recommendation files in the following format:
		"item1",("user_reco1",score),("user_reco2",score) ...
		"item2",("user_reco1",score),("user_reco2",score) ...

"""

from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, Rating
import sys
import csv


def main(data_source, users_source, apps_source, output, number_recs):

    users_data = {}
    with open(users_source) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            users_data[int(row[0])] = row[1]

    apps_data = {}
    with open(apps_source) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            apps_data[int(row["id"])] = row["app"]

    # This should be changed if running on cluster
    conf = SparkConf().setMaster("local[*]").setAppName("AptoideALS")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")
    # Load and parse the data
    data = sc.textFile(data_source)
    ratings = data.map(lambda l: l.split(','))\
        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()

    # Build the recommendation model using Alternating Least Squares
    seed = 5L
    iterations = 10
    # Is a basic L2 Regularizer to reduce overfitting
    regularization_parameter = 0.1
    # Number of features used to describe items
    rank = 50
    # Is the confidence that we have that the user likes the item
    alpha = 100.0

    model = ALS.trainImplicit(ratings,
                              rank,
                              seed=seed,
                              iterations=iterations,
                              lambda_=regularization_parameter,
                              alpha=alpha)

    # Evaluate the model on training data
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = " + str(MSE))

    users_names = sc.broadcast(users_data)
    apps_names = sc.broadcast(apps_data)

    def construct_string(x):
        item = "\"{}\"".format(str(apps_names.value[x[0]]))
        recs = [("\"{}\"".format(str(users_names.value[y[1]])), y[2]) for y in x[1]]
        string = "{},{}".format(item, str(recs))
        return string.replace("[", "").replace("]", "").replace("\\", "").replace("'", "").replace(" ", "")

    recRDD = model.recommendUsersForProducts(int(number_recs)).map(construct_string)

    recRDD.saveAsTextFile(output)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
