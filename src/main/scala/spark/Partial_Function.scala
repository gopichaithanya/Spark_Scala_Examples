package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Partial_Function extends App{

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark = SparkSession.builder().master("local[3]").appName("Spark_App").getOrCreate()


}
