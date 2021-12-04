package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SearchingDataFrame extends App{

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark = SparkSession.builder().appName("Spark_App0").master("local[3]").getOrCreate()
  import spark.implicits._
  val testDF = Seq((1,"Jhon Smith"), (2,"Michael Munna"), (3,"Bob Williamson"), (4,"Jack Rose"),(5,"Bob Williamson"), (6, "Rob Williamson")
  ).toDF("ID", "Name")
  testDF.filter(col("name").contains("Williamson")).show()
  testDF.filter(col("name").like("%Williamson")).show()
  testDF.filter(col("name").rlike("Bob|Rob")).show()
  testDF.filter(col("name").rlike("Bob|Rob")).show()


}
