from pyspark import SparkContext, SparkConf
from user_definition import *
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SQLContext
# Do not add additional libraries.

# Create SparkContext. - You can change this, if you want.
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

########## 
# Load data
itComentsRows = sc.textFile(file_name)
itCommentsSplit = itComentsRows.map(lambda x : x.split("~"))

# Define safe type coertions
def toIntSafe(inval):
  try:
    return int(inval)
  except ValueError:
    return None

def toTimeSafe(inval):
  try:
    return datetime.strptime(inval, "%Y-%m-%d %H:%M:%S.%f")
  except ValueError:
    return None

# Define schema
commentSchema = StructType([
    StructField("id", IntegerType(), False),
    StructField("postId", IntegerType(), False),
    StructField("time", TimestampType(), True),
    StructField("voteType", IntegerType(), True)
    ])

# Define map function
def stringToComment(r):
  return Row(
    toIntSafe(r[0]),
    toIntSafe(r[1]),
    toTimeSafe(r[3]),
    toIntSafe(r[2])
    )

# Create RDD using map function
rowRDD = itCommentsSplit.map(lambda x: stringToComment(x))

# Transform to DF using the schema
itCommentsDFStruct = sqlContext.createDataFrame(rowRDD, commentSchema)

# Print schema (no need for <print> statement)
itCommentsDFStruct.printSchema()

# Print "postId" of given "id" field (no need for <print> statement)
itCommentsDFStruct.filter("id = {}".format(id)).select("postId").show()

##########
sc.stop()
