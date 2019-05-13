from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD as lrSGD

spark = SparkSession \
    .builder \
    .appName("Python Spark regression example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

regressionDataFrame = spark.read.csv('Advertising.csv',header=True, inferSchema = True)

regressionDataFrame = regressionDataFrame.drop('_c0')

regressionDataFrame.show(10)

regressionDataRDD = regressionDataFrame.rdd.map(list)


regressionDataLabelPoint = regressionDataRDD.map(lambda data : LabeledPoint(data[3], data[0:3]))

regressionLabelPointSplit = regressionDataLabelPoint.randomSplit([0.7, 0.3])

regressionLabelPointTrainData = regressionLabelPointSplit[0]

regressionLabelPointTestData = regressionLabelPointSplit[1]


ourModelWithLinearRegression  = lrSGD.train(data = regressionLabelPointTrainData, 
                                            iterations = 200, step = 0.02, intercept = True)
print(ourModelWithLinearRegression.weights)