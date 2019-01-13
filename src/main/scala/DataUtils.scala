import org.apache.spark.sql
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.{DataType, DataTypes}

class DataUtils extends Serializable {

  def normalizeColumnName(data: sql.DataFrame): sql.DataFrame = {
    var newData = data
    data.columns.foreach(col => {
      newData = newData.withColumnRenamed(col, col.replaceAll("[\\s / ,]", "_").toLowerCase())
    })
    return newData
  }

  def updateDataFrameSchema(data: sql.DataFrame, columnName: String, newColumnType: DataType ): sql.DataFrame = {
    var updatedData = data
    newColumnType match {
      case DataTypes.DateType =>
        updatedData = updatedData.withColumn(columnName, to_date(updatedData(columnName),"dd-mm-yy"))
      case _ =>
        updatedData = updatedData.withColumn(columnName, updatedData(columnName).cast(newColumnType))
    }
    return  updatedData
  }

}
