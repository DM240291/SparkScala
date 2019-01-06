import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, sql}

object SparkScala extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

//  val sparkConf = new SparkConf().setMaster("local").setAppName("MySparkCode")
//
//  val spark = new SparkContext(sparkConf)

  val sparkSession = new sql.SparkSession.Builder().master("local").appName("MySparkSql").getOrCreate()

  val data = sparkSession.read.format("csv").option("header", true).load("./src/main/resources/Data.txt")

  data.show()

}
