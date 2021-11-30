package spark

import org.apache.spark.sql.SparkSession

import java.util.Scanner

object Spark_Scala04 extends App{

  val spark= SparkSession.builder().master("local[3]").appName("spark-App").getOrCreate()
  println(spark.sparkContext)
  import spark.implicits._
  val randomInt=scala.util.Random.nextInt(10000)
  val dataframeFirst=spark.sparkContext.parallelize(
    Seq.fill(100000){(randomInt,randomInt,randomInt)}).toDF("c1First","c2First","c3First")
  val dataframeSecond=spark.sparkContext.parallelize(
 Seq.fill(100000){(randomInt,randomInt,randomInt)}
  ).toDF("c1Sec","c2Sec","c3Sec")

  dataframeFirst.show()
  dataframeSecond.show()

  val resultDFOR= dataframeFirst.join(dataframeSecond,$"c1First"===$"c2Sec"|| $"c2First"===$"c2Sec")
  resultDFOR.show()
  resultDFOR.explain()



}
