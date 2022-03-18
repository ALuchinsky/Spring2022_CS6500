
{
 import java.nio.file.Paths;
 val path = Paths.get("data/tab.json").toAbsolutePath().toString();
 val tab = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("file://"+path)
 val rows = tab.filter( tab("x")>0).select("str").collect()
 val words = rows.map( s => s(0).toString().split(" ")).flatMap(_.toList)
 print("["+words.mkString(",")+"]")
}
 
 