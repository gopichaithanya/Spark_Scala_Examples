package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Spark_Scala05 extends App{

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark= SparkSession.builder().master("local[3]").appName("Spark_App").getOrCreate()
  println("Spark Processing Started ........")
  val dataDF = Seq(Row(1231,"Chaitanya","Sfw",30000.00,"Hyderabad"),
    (Row(1232,"Ravi","Sfw",25000.00,"Hyderabad")))


  val data_schema= new StructType()
    .add("Employee_Id",IntegerType,true)
    .add("Employee_Name",StringType,true)
    .add("job",StringType,true)
    .add("Salary",DoubleType,true)
    .add("City",StringType,true)

  val resultDF = spark.createDataFrame(spark.sparkContext.parallelize(dataDF),data_schema)
  resultDF.show()
}
