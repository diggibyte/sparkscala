import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

object Assignment extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()
  val data = Seq(Row("rahul",10,"M",30000),
    Row("rohit",20,"M",20000),
    Row("ram",30,"M",30000),
    Row("reshma",40,"F",11000),
    Row("rose",50,"F",10000))
  val Schema = Service.NewSchema()
  val data1 = spark.createDataFrame(spark.sparkContext.parallelize(data),Schema)
  data1.printSchema()  // printing the schema
  data1.show()

  //Q1
  Service.average(data1).show()
  //Q2
  Service.employee_count(data1).show()
}
