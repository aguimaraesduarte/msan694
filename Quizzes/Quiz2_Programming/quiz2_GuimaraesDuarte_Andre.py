from pyspark import SparkContext, SparkConf
from user_definition import *

# Create SparkContext
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf=conf)

# Read Data
lines = sc.textFile(file_name)

# Pair RDD
pairs = lines.map(lambda x: (len(x.split(",")[0]), 1))
pairs_sum = pairs.reduceByKey(lambda x, y: x+y)

# Sort and collect
print pairs_sum.sortByKey().collect()

# Stop Spark
sc.stop()