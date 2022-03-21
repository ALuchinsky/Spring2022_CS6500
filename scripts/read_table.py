import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


findspark.init()
spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()

df = spark.read.option("multiLine", True).option(
    "mode", "PERMISSIVE").json("/input/tab.json")

df2 = df. \
    filter(df.x > 0).                               \
    withColumn("str", split(col("str"), " ")).      \
    select(explode(col("str")).alias("word")).      \
    groupBy("word").count()

df2.show()
