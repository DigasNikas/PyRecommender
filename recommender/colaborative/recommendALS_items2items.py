"""
By Diogo Nicolau

Recommendation engine based on Spark mllib ALS and Annoy
Recommends a list of items to every item in the dataset

Input:
	- csv dataset;
	- csv id -> app_names;
	- number of recommendations per app
Output:
	- recommendation files in the following format:
		"app1",("app_reco1",score),("app_reco2",score) ... 
		"app2",("app_reco1",score),("app_reco2",score) ... 

"""

from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, Rating
from pyspark import SparkFiles
from annoy import AnnoyIndex
import sys
import csv


def main(data_source, names_source, output, number_recs):

    names_data = {}
    with open(names_source) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            names_data[int(row["id"]) - 1] = row["app"]
            # indexes in annoy start at 0

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
    items = model.productFeatures().collect()
    for i, vector in items:
        # Annoy start at index 0, while Spark starts at index 1. We need to -1 every index
        index.add_item(i-1, vector)
    # n_trees
    index.build(300)
    index.save("index.ann")
    sc.addPyFile("index.ann")

    # Broadcast: improve performance by sending once per node rather than a once per task
    names = sc.broadcast(names_data)

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
                array.append("(\"{}\",{})".format(names.value[item], str(order)))
                order -= 1
        result = "\"{}\",{}".format(names.value[x[0]], str(array)).replace(" ", "").replace("[", "").replace("]", "").replace("'", "")
        return result

    similarRDD = model.productFeatures().mapPartitions(find_neighbors)
    similarRDD.map(construct_string).saveAsTextFile(output)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

    '''

        # Broadcast: improve performance by sending once per node rather than a once per task
        names = sc.broadcast(names_data)

        def fn(x):
            return names.value[x[0]], Vectors.dense(x[1])

        items_featuresRDD = model.productFeatures().map(fn).cache()
        items_featuresRDD.take(1)
        items = sc.broadcast(items_featuresRDD.collect())
        recs = sc.broadcast(int(number_recs))

        # Function to convert into the format required
        # Need to convert inside the RDD so it make us of spark's file writer
        def construct_string(key, array):
            result = ("\"" + str(key) + "\"," + str(sorted(array, key=(lambda y: y[1]), reverse=True)[:recs.value])) \
                .replace(" ", "") \
                .replace("[", "") \
                .replace("]", "") \
                .replace("'", "\"")
            return result

        def cosine_similarity(v1, v2):
            "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"
            sumxx, sumxy, sumyy = 0, 0, 0
            for i in range(len(v1)):
                x = v1[i]
                y = v2[i]
                sumxx += x * x
                sumyy += y * y
                sumxy += x * y
            return sumxy / math.sqrt(sumxx * sumyy)

        # Compute the square distance between the rdd vectors and the broadcasted vector
        def func(x):
            key = x[0]
            array = []
            for var in items.value:
                if key != var[0]:
                    squared_distance = cosine_similarity(x[1], var[1])
                    # squared_distance = Vectors.squared_distance(x[1], var[1])
                    array.append((var[0], squared_distance))
            return construct_string(key, array)

        items_featuresRDD.map(func).saveAsTextFile(output)
    '''

    '''
    keys = items_featuresRDD.countByKey()
    print "Number of items: " + str(len(keys))
    cartesianRDD = items_featuresRDD.cartesian(items_featuresRDD).filter(lambda x: x[0][0] != x[1][0]).cache()
    print "Cartesian Size: " + str(cartesianRDD.count())

    def fn(x):
        nor = Normalizer(2)
        v1 = nor.transform(x[0][1])
        v2 = nor.transform(x[1][1])
        key = x[0][0]
        value = x[1][0]
        squared_distance = Vectors.squared_distance(v1, v2)
        return key, (value, squared_distance)

    squared_distanceRDD = cartesianRDD.map(fn).cache()
    squared_distanceRDD.take(1)
    print "Squared Distance OK"

    with open("results2.txt", 'w') as outf:
        for key in keys:
            print "Computing " + str(key)
            item_results = squared_distanceRDD.filter(lambda x: x[0] == key)\
                .map(lambda (x, y): y)\
                .takeOrdered(10, lambda (x, y): y)
            outf.write(str(key) + "," + str(item_results).replace("[", "").replace("]", "").replace(" ", "") + "\n")
    '''

    '''
    # Second approach, not being used due memory consumption
    # We collect the items features and transpose it using numpy
    # Then create a new RDD from the transposed item features array
    tems_featuresVectorsRDD = items_featuresRDD.map(lambda r: r[1])
    items_features = items_featuresVectorsRDD.collect()
    transpose = np.asarray(items_features).transpose()
    transposeRDD = sc.parallelize(transpose)
    itemsMatrix = RowMatrix(transposeRDD)

    # Sparse Vectors might enhance the performance
    size = transpose[0].size
    index = np.arange(size)
    itemsMatrix = RowMatrix(transposeRDD.map(lambda r: Vectors.sparse(size, index, r)))
    similaritiesRDD = itemsMatrix.columnSimilarities()
    print similaritiesRDD.entries.take(1)

    # Save and load model
    model.save(sc, "target/tmp/myCollaborativeFilter")
    saveModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
    '''
