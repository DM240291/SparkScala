import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ClimateStudy extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkSession = new sql.SparkSession.Builder().master("local").appName("AQI_Data").getOrCreate()

  val sparkUtils = new DataUtils

  var data = sparkSession.read
    .format("csv")
    .option("header", true)
    .load("./src/main/resources/Delhi_AQI_2005.csv")

  data = sparkUtils.normalizeColumnName(data)

  data = sparkUtils.updateDataFrameSchema(data, "sampling_date", DataTypes.DateType)

  data.show()

}
