package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkTest extends App with Context{

   Logger.getLogger("org.apache").setLevel(Level.WARN)


}
