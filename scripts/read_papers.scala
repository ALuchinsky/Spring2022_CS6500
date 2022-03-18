import java.nio.file.Paths;

// Loading dataset
val path = Paths.get("data/AL_papers.json").toAbsolutePath().toString();
val papers = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("file://"+path)

// extracting papers list
val paps = papers.select(explode(col("hits.hits")).as("paper"))

// extracting titles
val titles = paps.select(explode(col("paper.metadata.titles").as("title"))).select("col.title")
// flat list of all the words from titles
//val kws_tits = titles.map(s => s(0).toString().split(" ")).flatMap(_.toList)
val kws_tits = titles.
    select("title").                        // extract title
    map(s => s.toString().split(" ")).      // split to words
    select(explode(col("value")).as("K")).  // wide to long
    groupBy("K").count().sort("count")      // count and sort

// extracting abstracts
val abstracts = paps.select( explode( col("paper.metadata.abstracts").as("abs"))).select("col.value")
// flat list af all the words from abstracts
//val kws_abs = abstracts.map(s => s.toString().split(" ")).flatMap(_.toList)
val kws_abs = abstracts.
    select("value").                            // extract abstract
    map(s => s.toString().split(" ")).          // split to words
    select( explode(col("value")).as("K")).     // wide to long
    groupBy("K").count().sort("count")          // count and sort

val authors = paps.select("paper.metadata.authors.full_name")
val all_authors = authors.select( explode(col("full_name"))).distinct()