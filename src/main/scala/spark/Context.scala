package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkConf=new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[3]")
    .set("spark.cores.max","2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

}
