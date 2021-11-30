package spark

import org.apache.spark.sql.SparkSession

trait SparkSupportMixin {


  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Spark Testing App")
    .getOrCreate()

}
