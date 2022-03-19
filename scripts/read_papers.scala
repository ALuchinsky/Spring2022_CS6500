import java.nio.file.Paths;

// Loading dataset
val path = Paths.get("data/AL_papers.json").toAbsolutePath().toString();
val papers = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("file://"+path)

// extracting papers list
val paps = papers.select(explode(col("hits.hits")).as("paper"))

val paps_short = paps.select(
    col("paper.metadata.titles"), 
    col("paper.metadata.abstracts"), col("paper.created"), col("paper.metadata.number_of_pages"), 
    col("paper.metadata.keywords"), col("paper.metadata.references"),
    col("paper.metadata.authors.full_name").as("authors")
    )

val kws_assigned = paps_short.select(explode(col("keywords"))).
    select("col.value").groupBy("value").count().
    sort(desc("count"))

var kws_title = paps_short.
    select(explode(col("titles"))).select("col.title").     // extract title
    map(T => T.toString().split(" ")).                      // split to words
    select(explode(col("value")).as("K")).                  // wide to long
    groupBy("K").count().sort("count")                      // count and sort

 val kws_abstracts = paps_short.
    select(explode(col("abstracts"))).select("col.value").  // extract title
    map(T => T.toString().split(" ")).                      // split to words
    select(explode(col("value")).as("K")).                  // wide to long
    groupBy("K").count().sort("count")                      // count and sort

val all_authors = paps_short.
    select(explode(col("authors")).as("full_name")).
    groupBy("full_name").count().
    sort(desc("count"))

val total =  papers.select("hits.total").collect()(0)(0).toString.toInt
