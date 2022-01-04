import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class TestAssignment extends AnyFunSuite{

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()

  val input = Seq(Row("Revathi", 13, "F", 30000),
    Row("Rama", 12, "M", 50000),
    Row("Krishna", 14, "M", 40000),
    Row("Kranthi", 15, "F", 40000))

  //Solution
  val schema: StructType = Service.NewSchema()
  val data: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(input), schema)

  import spark.implicits._

  //Solution 1
  assert(Service.average(data).collect().toList===List((40000)).toDF.collect().toList)

  //Solution 2
  assert(Service.employee_count(data).collect().toList===List((4)).toDF.collect().toList)
}
