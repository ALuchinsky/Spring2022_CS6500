import findspark
import pyspark
from pyspark.sql import SparkSession

findspark.init()


spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()
df = spark.read.json(
    "file:///2022_SP_6500_FP_Gu_Luchinsky_Mitchell/data/tab.json")
ddf = df.toPandas()
words_ = ddf.query("x>0")["str"].map(lambda s: s.split()).tolist()
words = [item for sublist in words_ for item in sublist]
print(words)
