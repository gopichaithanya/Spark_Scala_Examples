package spark

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.Random
import scala.collection.immutable

object KeyGenerator {
  val random = new Random()

  def getKeys(useSkewed: Boolean,
              oneKeyDominant: Boolean)  : immutable.Seq[Int] = {

    def genKeys(howMany: Int,
                lowerInclusive: Int,
                upperExclusive: Int)  = {
      (1 to howMany).map{ i =>
        lowerInclusive +
          random.nextInt(upperExclusive - lowerInclusive)
      }
    }

    val keys  =
      if (useSkewed) {
        val skewedKeys =
          if (oneKeyDominant)
            Seq.fill(55)(0)
          else
            Seq.fill(18)(0) ++ Seq.fill(18)(2) ++ Seq.fill(19)(4)

        genKeys(45, 1, 45) ++ skewedKeys
      }
      else {
        genKeys(100, 1, 100)
      }

    System.out.println("keys:" + keys);
    System.out.println("keys size:" + keys.size);

    keys
  }
}

object SaltedToPerfection extends App {

  import KeyGenerator._
  def runApp(numPartitions: Int,
             useSkewed: Boolean,
             oneKeyDominant: Boolean) = {

    val keys: immutable.Seq[Int] = getKeys(useSkewed, oneKeyDominant)
    val keysRdd: RDD[(Int, Int)] =
      sparkSession.sparkContext.
        parallelize(keys).map(key => (key,key)). // to pair RDD
        partitionBy(new HashPartitioner(numPartitions))


    System.out.println("keyz.partitioner:" + keysRdd.partitioner)
    System.out.println("keyz.size:" + keysRdd.partitions.length)

    def process(key: (Int, Int), index: Int): Unit = {
      println(s"processing $key in partition '$index'")
      Thread.sleep(50)     // Simulate processing w/ delay
    }

    keysRdd.mapPartitionsWithIndex{
      (index, keyzIter) =>
        val start = System.nanoTime()
        keyzIter.foreach {
          key =>
            process(key, index)
        }
        val end = System.nanoTime()
        println(
          s"processing of keys in partition '$index' took " +
            s" ${(end-start) / (1000 * 1000)} milliseconds")
        keyzIter
    }
      .count
  }


  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[4]")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()


  val numPartitions = 50
  val useSkewed = true
  val oneKeyDominant = true

  runApp(numPartitions, useSkewed, oneKeyDominant)
  Thread.sleep(1000 * 600)    // 10 minutes sleep to explore with UI


}