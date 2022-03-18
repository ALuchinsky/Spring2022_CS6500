
{
 import java.nio.file.Paths;

 // Reading JSON file to a DataFrame. Note that I do not specify the schema of the file
 val path = Paths.get("data/tab.json").toAbsolutePath().toString();
 val tab = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("file://"+path)

// Filter rows by x and extract the str column
 val rows = tab
    .filter( tab("x")>0)
    .select("str")
    .collect()

 // Split all strs  into words and flatten the Array
 val words = rows.map( s => s(0).toString().split(" "))
    .flatMap(_.toList)

 // printing the result
 print("["+words.mkString(",")+"]")
}
 
 