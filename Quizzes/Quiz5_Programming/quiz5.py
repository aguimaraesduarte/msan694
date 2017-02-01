from pyspark import SparkContext, SparkConf
from user_definition import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
#Do not add additional libraries.

#Create SparkContext. - You can change this, if you want.
conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

##########

#########
sc.stop()
