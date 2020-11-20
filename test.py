import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("us_census.csv")

df.show(5)