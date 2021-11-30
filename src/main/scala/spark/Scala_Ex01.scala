package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Scala_Ex01 extends App{

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark = SparkSession.builder().master("local[3]").appName("spark-app01").getOrCreate()
  println(spark)
  import spark.implicits._
  val dataDF=spark.sparkContext.textFile("data//word_count.txt")
  val wordCountDF=dataDF.flatMap(line => line.split(" ")).map(word=>(word,1)).reduceByKey(_+_).toDF("words","count")
  wordCountDF.show()





}
