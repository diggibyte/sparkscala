import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Service
{
  def NewSchema() : StructType ={
    new StructType()
      .add("Name", StringType)
      .add("ID", IntegerType)
      .add("Gender", StringType)
      .add("Salary", IntegerType)
  }
  def average(dataDF:DataFrame):DataFrame={
    val average = dataDF
    average.select(avg("Salary").as("Avg_Salary"))
  }
  def employee_count(dataDF:DataFrame):DataFrame={
    val employee_count = dataDF
    employee_count.select(count("Name").as("employee_count"))
  }

}