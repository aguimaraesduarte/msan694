from pyspark import SparkContext, SparkConf
from user_definition import *
# Do not add any additional libraries.

# Create SparkContext
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf=conf)

# Read Data
lines = sc.textFile(input_file)

# Convert to flatmap of <int>
nums = lines.flatMap(lambda l: [int(i) for i in l.split(",")])

# Get RDDs for even and odd numbers
even_nums = nums.filter(lambda a: a%2 == 0)
odd_nums = nums.filter(lambda a: a%2 == 1)

# Get histograms
print even_nums.histogram(hist_size)
print odd_nums.histogram(hist_size)

# Stop Spark
sc.stop()