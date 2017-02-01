from pyspark import SparkContext, SparkConf
from user_definition import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
#Do not add additional libraries.


##########
def fun(x):
    if x[1]==5:
        return x[0]

supervisor = sc.textFile(file_name_2).map(lambda x: (x.split(","), len(x.split(",")[0])))
supervisor = supervisor.map(lambda x: fun(x))
business = sc.textFile(file_name_1).map(lambda x: (x.split(","), len(x.split(",")[0])))
business = business.map(lambda x: fun(x))

supervisorSchema = StructType([
    StructField("zip", IntegerType(), False),
    StructField("supervisor", IntegerType(), True)
])

businessSchema = StructType([
    StructField("zip", IntegerType(), False),
    StructField("business_name", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

def stringToSup(r):
  return (int(r[0]), int(r[1]))

def stringToBus(r):
  return (
    int(r[0]),
    r[1],
    r[2],
    r[3],
    r[4]
)

# Create RDD using map function
supervisorRDD = supervisor.map(lambda x: stringToSup(x))
businessRDD = business.map(lambda x: stringToBus(x))

# Transform to DF using the schema
supervisorStructDF = sqlContext.createDataFrame(supervisorRDD, supervisorSchema)
businessStructDF = sqlContext.createDataFrame(businessRDD, businessSchema)

# Join tables
joinedDF = supervisorStructDF.join(businessStructDF, "zip", "inner").distinct()

# print schema
joinedDF.printSchema()

# number of rows of given supervisor_id
#print joinedDF.groupBy("supervisor").count().filter("supervisor = {}".format(supervisor_id)).collect()
print "commented out. Had an error :("

# zip, business_name, city of supervisor_id
#joinedDF.filter("supervisor = {}".format(supervisor_id)).select("zip", "business_name", "city").sort(["zip", "business_name", "city"], ascending = True).show(10)
print "commented out. Had an error :("

#########
sc.stop()
