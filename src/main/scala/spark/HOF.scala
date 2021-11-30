package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object HOF extends App{

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark=SparkSession.builder().master("local[3]").appName("spark_App").getOrCreate()
  println(spark)
  val arrayStructureData = Seq(
    Row("James,,Smith",List("Java","Scala","C++"),List("Spark","Java"),"OH","CA"),
    Row("Michael,Rose,",List("Spark","Java","C++"),List("Spark","Java"),"NY","NJ"),
    Row("Robert,,Williams",List("CSharp","VB"),List("Spark","Python"),"UT","NV")
  )
  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("languagesAtWork", ArrayType(StringType))
    .add("currentState", StringType)
    .add("previousState", StringType)
  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df.printSchema()
  df.show()

  val arrayStructure1 = Seq(
     Row(1,List("CSharp","VB","Java","C#"))
  )
  val ar_schema = new StructType()
    .add("Id",IntegerType,true)
    .add("language",ArrayType(StringType),true)
  val df1= spark.createDataFrame(spark.sparkContext.parallelize(arrayStructure1),ar_schema)
  df1.printSchema()
  df1.show()

}
