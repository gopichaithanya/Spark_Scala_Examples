package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class Order(price:Double,qty:Int)
object HOF1 extends App{
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark =SparkSession.builder().master("local[3]").appName("Spark_App").getOrCreate()
  
}
