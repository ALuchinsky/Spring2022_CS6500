import java.nio.file.Paths;

// Loading dataset
val path = Paths.get("data/AL_papers.json").toAbsolutePath().toString();
val papers = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("file://"+path)

// extracting papers list
val paps = papers.select(explode(col("hits.hits")).as("paper"))

// extracting titles
val titles = paps.select(explode(col("paper.metadata.titles").as("title"))).select("col.title")
// flat list of all the words from titles
val kws_tits = titles.map(s => s(0).toString().split(" ")).flatMap(_.toList)

// extracting abstracts
val abstracts = paps.select( explode( col("paper.metadata.abstracts").as("abs"))).select("col.value")
// flat list af all the words from abstracts
val kws_abs = abstracts.map(s => s.toString().split(" ")).flatMap(_.toList)
