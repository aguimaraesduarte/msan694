from pyspark import SparkContext, SparkConf
from user_definition import *
# Do not add any additional libraries.

lines = sc.textFile(input_file)
nums = lines.flatMap(lambda l: [int(i) for i in l.split(",")])
even_nums = nums.filter(lambda a: a%2 == 0)
odd_nums = nums.filter(lambda a: a%2 == 1)
print even_nums.histogram(hist_size)
print odd_nums.histogram(hist_size)