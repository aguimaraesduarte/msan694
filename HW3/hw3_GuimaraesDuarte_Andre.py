from pyspark import SparkContext, SparkConf
from user_definition import *
# Do not add any additional libraries.

# Create SparkContext
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf=conf)

# Read Data
lines = sc.textFile(input_file)

# Transform
sf_zip = lines.map(lambda x: x.split(concat_string))
sf_zip_group = sf_zip.groupByKey()
sf_zip_group_list = sf_zip_group.map(lambda x: (x[0], list(x[1])))

# Get entry with most supervisors
zip_max = sf_zip_group_list.sortBy(lambda x: len(x[1]), ascending=False).first()

# zipcode with the maximum number of supervisors
print zip_max[0]

# its list of supervisors
print concat_string.join(zip_max[1])

# the number of supervisors
print len(zip_max[1])

# Stop Spark
sc.stop()