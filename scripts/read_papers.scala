import java.nio.file.Paths;

// Loading dataset
val path = Paths.get("data/AL_papers.json").toAbsolutePath().toString();
val papers = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("file://"+path)

 val common_words = spark.read.csv("file://"+
    Paths.get("data/4000-most-common-english-words-csv.csv").toAbsolutePath().toString()
 );


// extracting papers list
val paps = papers.select(explode(col("hits.hits")).as("paper"))

val paps_short = paps.select(
    element_at($"paper.metadata.titles.title", 1).as("title"), 
    element_at($"paper.metadata.abstracts.value", 1).as("abstract"), 
    col("paper.created"), col("paper.metadata.number_of_pages"), 
    col("paper.metadata.keywords"), size(col("paper.metadata.references")).as("num_refs"),
    col("paper.metadata.authors.full_name").as("authors")
    )

val kws_assigned = paps_short.select(explode(col("keywords"))).
    select("col.value").groupBy("value").count().
    sort(desc("count"))

val kws_title = paps_short.select("title").
    map(T => T.toString.split(" ")).        // split to words        
    select(explode(col("value")).as("K")).  // wide to long
    except(common_words).                   // not counting common words
    groupBy("K").count().sort("count")      // sort and count

val kws_abstracts = paps_short.select("abstract").
    map(T => T.toString.split(" ")).        // split to words        
    select(explode(col("value")).as("K")).  // wide to long
    except(common_words).                   // not counting common words
    groupBy("K").count().sort("count")      // sort and count

// var kws_title = paps_short.
//     select(explode(col("titles"))).select("col.title").     // extract title
//     map(T => T.toString().split(" ")).                      // split to words
//     select(explode(col("value")).as("K")).                  // wide to long
//     except(common_words).                                   // not counting common words
//     groupBy("K").count().sort("count")                      // count and sort

// val kws_abstracts = paps_short.
//     select(explode(col("abstracts"))).select("col.value").  // extract title
//     map(T => T.toString().split(" ")).                      // split to words
//     select(explode(col("value")).as("K")).                  // wide to long
//     except(common_words).                                   // not counting common words
//     groupBy("K").count().sort("count")                      // count and sort

val all_authors = paps_short.
    select(explode(col("authors")).as("full_name")).
    groupBy("full_name").count().
    sort(desc("count"))

val total =  papers.select("hits.total").collect()(0)(0).toString.toInt
