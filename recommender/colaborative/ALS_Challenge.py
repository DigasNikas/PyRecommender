"""
By Diogo Nicolau

Recommendation engine based on Spark mllib ALS
Recommends a list of users to every item in the dataset

Input:
	- csv dataset;
	- targetItems.csv
	- number of recommendations per item
Output:
	- recommendation files in the following format:
		item1 user_reco1,user_reco2 ...
		item2 user_reco1,user_reco2 ...

"""

from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, Rating
import sys
import csv

def main(data_source, items_source, output, number_recs):

    # This should be changed if running on cluster
    conf = SparkConf().setMaster("local[*]").setAppName("RecSysALS")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")
    # Load Target Users

    #target_items = sc.textFile(items_source)

    with open(items_source) as csvfile:
        reader = csv.reader(csvfile)
        target_items = list(reader)


    def func(x):
        val = x[2]
        # if printed
        if float(x[2]) == 0.0:
            val = 1
        # if clicked
        if float(x[2]) == 1.0:
            val = 2
        #if bookmarked
        if float(x[2]) == 2.0:
            val = 5
        #if replied
        if float(x[2]) == 3.0:
            val = 5
        #if deleted
        if float(x[2]) == 4.0:
            val = -10
        #if recruiter interest
        if float(x[2]) == 5.0:
            val = 20
        return Rating(int(x[0]), int(x[1]), float(val))

    # Load and parse the data
    data = sc.textFile(data_source)
    ratings = data.map(lambda l: l.split(',')).map(func).cache()

    # Build the recommendation model using Alternating Least Squares
    seed = 5L
    iterations = 12
    # Is a basic L2 Regularizer to reduce overfitting
    regularization_parameter = 0.000001
    # Number of features used to describe items
    rank = 110
    # Is the confidence that we have that the user likes the item
    alpha = 1000.0

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

    #recRDD = target_items.map(lambda x: model.recommendUsers(int(x), int(number_recs)))
    #recRDD.saveAsTextFile("final.txt")

    with open(output, 'w') as outf:
        erro = 0
        for target in target_items:
            try:
                rec = model.recommendUsers(int(target[0]), int(number_recs))
                outf.write(str(target[0])+"\t")
                k = 0
                for elements in rec:
                    k += 1
                    outf.write(str(elements[0]))
                    if k < len(rec):
                        outf.write(",")
                    else:
                        outf.write("\n")
            except:
                erro += 1

        print erro


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
