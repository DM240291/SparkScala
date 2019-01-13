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

  //  Find number of people working in management
  def getPeopleWorkingInDepartment(data: sql.DataFrame, departmentName: String): Int = {
    val dataCount = data.groupBy("job").count().filter("job = '" + departmentName + "'")
    val finalCount = String.valueOf(dataCount.select("count").first().get(0))
    return Integer.valueOf(finalCount)
  }

  val countData = this.getPeopleWorkingInDepartment(data, "technician")

  //  print(countData)

  //  Find the number of people working as entrepreneur and are less than 40 years old

  val ageWiseData = data.filter("age < 40 and job = 'entrepreneur'").count()

  print(ageWiseData)
}
