import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')


ascii_udf = udf(ascii_ignore)

findspark.init()
spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()
papers = spark.read.option("multiLine", True).option(
    "mode", "PERMISSIVE").option("encoding", "ascii").json("/input/AL_papers.json")
common_words = spark.read.text("/input/4000-most-common-english-words-csv.csv")

paps = papers.select(explode(col("hits.hits")).alias("paper"))

paps_short = paps.select(
    element_at(col("paper.metadata.titles.title"), 1).alias("title"),
    element_at(col("paper.metadata.abstracts.value"), 1).alias("abstract"),
    col("paper.created"), col("paper.metadata.number_of_pages"),
    col("paper.metadata.keywords"), size(
        col("paper.metadata.references")).alias("num_refs"),
    col("paper.metadata.authors.full_name").alias("authors")
).withColumn("title", ascii_udf("title"))

kws_title = paps_short.select("title").                \
    withColumn("title", split(col("title"), " ")).     \
    select(explode(col("title")).alias("K")).          \
    groupBy("K").count().sort(asc("count"))

kws_abstracts = paps_short.select("abstract").                \
    withColumn("abstract", split(col("abstract"), " ")).      \
    select(explode(col("abstract")).alias("K")).          \
    groupBy("K").count().sort(asc("count"))

paps_short.limit(10).select("title").show(truncate=False)

kws_title.limit(10).show()
