package spark

import org.apache.log4j.{Level, Logger}

object Spark_Scala01 extends App with Context{

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val dfTags = sparkSession
    .read.option("header","true")
    .option("inferSchema","true")
    .csv("data//question_tags_10K.csv")
    .toDF("id","tag")
  dfTags.show(10)

  dfTags.printSchema()
  dfTags.select("id", "tag").show(10)
  dfTags.filter("tag == 'php'").show(10)
  println(s"Number of php tags = ${ dfTags.filter("tag == 'php'").count() }")
  dfTags.filter("tag like 's%'").show(10)
  dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)
  dfTags.filter("id in (25, 108)").show(10)
  println("Group by tag value")
  dfTags.groupBy("tag").count().show(10)
  dfTags.groupBy("tag").count().filter("count > 5").show(10)
  dfTags.groupBy("tag").count().filter("count > 5").orderBy("tag").show(10)
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("data//questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")
  dfQuestionsCSV.printSchema()
  dfQuestionsCSV.show(10)
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )
  dfQuestions.printSchema()
  dfQuestions.show(10)
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()
  dfQuestionsSubset.show()
  dfQuestionsSubset.join(dfTags, "id").show(10)
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "inner")
    .show(10)
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "left_outer")
    .show(10)
  dfTags
    .join(dfQuestionsSubset, Seq("id"), "right_outer")
    .show(10)
  dfTags
    .select("tag")
    .distinct()
    .show(10)



}
