"""
By Diogo Nicolau

Recommendation engine based on Spark mllib ALS and Annoy
Recommends a list of users to every user in the dataset

Input:
	- csv dataset;
	- csv id -> users_names;
	- number of recommendations per app
Output:
	- recommendation files in the following format:
		"user1",("user_reco1",score),("user_reco2",score) ...
		"user2",("user_reco1",score),("user_reco2",score) ...

"""

from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, Rating
from pyspark import SparkFiles
from annoy import AnnoyIndex
import sys
import csv


def main(data_source, users_source, output, number_recs):

    users_data = {}
    with open(users_source) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            users_data[int(row[0])-1] = row[1]

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

    # Use Spotify annoy to get items neighbors based on the feature vectors
    # n_trees -> a larger value will give more accurate results, but larger indexes
    # search_k -> a larger value will give more accurate results, but will take longer time to return
    index = AnnoyIndex(rank, 'angular')
    items = model.userFeatures().collect()
    for i, vector in items:
        # Annoy start at index 0, while Spark starts at index 1. We need to -1 every index
        index.add_item(i-1, vector)
    # n_trees
    index.build(300)
    index.save("index.ann")
    sc.addPyFile("index.ann")

    # Broadcast: improve performance by sending once per node rather than a once per task
    names = sc.broadcast(users_data)

    # Major function to get recommendations based on features vectors
    # assumes items are numbered 0 ... n-1
    def find_neighbors(iter):
        t = AnnoyIndex(rank)
        t.load(SparkFiles.get("index.ann"))
        # search_k
        return ((x[0]-1, t.get_nns_by_item(x[0]-1, int(number_recs))) for x in iter)

    # Function to convert into the format required
    # Need to convert inside the RDD so it make us of spark's file writer
    def construct_string(x):
        array = []
        order = int(number_recs)
        for item in x[1]:
            if item != x[0]:
                array.append("(\"" + names.value[item] + "\"," + str(order) + ")")
                order -= 1
        result = ("\"" + names.value[x[0]] + "\"," + str(array)).replace(" ", "").replace("[", "").replace("]", "").replace("'", "")
        return result

    similarRDD = model.productFeatures().mapPartitions(find_neighbors)
    similarRDD.map(construct_string).saveAsTextFile(output)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])