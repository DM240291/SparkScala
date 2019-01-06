import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SparkScala extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkConf = new SparkConf().setMaster("local").setAppName("MySparkCode")

  val spark = new SparkContext(sparkConf)
  //  val newRdd = spark.parallelize(Range(2,200,5))

  val data = spark.textFile("/Users/dmishra/downloads/Data.txt")

  val filteredData = data.filter(x => x == "management")
  print(filteredData)

}
