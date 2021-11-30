package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object PF1 extends App{
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark = SparkSession.builder().master("local[3]").appName("spark-app01").getOrCreate()
  println(spark)
 import spark.implicits._
  val df = Seq(1,2,3,4).toDF("value")
  df.filter(col("value")> 2).show
  df.filter(col("value")< 2).show


}