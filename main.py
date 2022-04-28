import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


spark = SparkSession.builder\
	.master("local").appName("hdfs_test").getOrCreate()

# booksSchema = StructType() \
#                     	.add("id", "integer")\
#                     	.add("book_title", "string")\
#                     	.add("publish_or_not", "string")\
#                     	.add("technology", "string")

booksdata=spark.read.csv("hdfs://localhost:9000/user/hdoop/data/housing.csv", header=True,  inferSchema=True)
booksdata.show(5)
