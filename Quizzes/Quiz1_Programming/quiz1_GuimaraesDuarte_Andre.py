from pyspark import SparkContext, SparkConf
from user_definition import *

# Create SparkContext
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf=conf)

# Read Data
lines = sc.textFile(file_name)

# Filter
has_word = lines.filter(lambda l: word.lower() in l.lower())

# Count
print has_word.count()

# Stop Spark
sc.stop()