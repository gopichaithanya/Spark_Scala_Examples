package spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class SparkSampleData {

  def getSampleDataFrame(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    var sequenceOfOverview = ListBuffer[(String, String, String, Integer)]()
    sequenceOfOverview += Tuple4("Western Digital", "006", "20200901", 1)
    sequenceOfOverview += Tuple4("Western Digital", "2", "20200901", 0)
    sequenceOfOverview += Tuple4("Western Digital", "3", "20200901", 1)
    sequenceOfOverview += Tuple4("Western Digital", "4", "20200901", 0)
    sequenceOfOverview += Tuple4("Western Digital", "1", "20200902", 1)
    sequenceOfOverview += Tuple4("Western Digital", "2", "20200902", 0)
    sequenceOfOverview += Tuple4("Western Digital", "3", "20200902", 1)
    sequenceOfOverview += Tuple4("Western Digital", "4", "20200902", 1)
    sequenceOfOverview += Tuple4("Western Digital", "1", "20200903", 0)
    sequenceOfOverview += Tuple4("Western Digital", "2", "20200903", 0)
    sequenceOfOverview += Tuple4("Western Digital", "3", "20200903", 0)
    sequenceOfOverview += Tuple4("Western Digital", "4", "20200903", 1)
    sequenceOfOverview += Tuple4("Western Digital", "1", "20200904", 0)
    sequenceOfOverview += Tuple4("Western Digital", "2", "20200904", 0)
    sequenceOfOverview += Tuple4("Western Digital", "3", "20200904", 1)
    sequenceOfOverview += Tuple4("Western Digital", "4", "20200904", 1)
    val df1 =
      sequenceOfOverview.toDF("Employee", "Id", "doj", "numAwards")
    df1
  }

}
