package spark

import org.apache.log4j.{Level, Logger}

object Spark_Scala02 extends App with Context {

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data//question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.createOrReplaceTempView("so_tags")
  sparkSession.catalog.listTables().show()
  sparkSession.sql("show tables").show()
  sparkSession
    .sql("select id, tag from so_tags limit 10")
    .show()
  sparkSession
    .sql("select * from so_tags where tag = 'php'")
    .show(10)
  sparkSession
    .sql(
      """select
        |count(*) as php_count
        |from so_tags where tag='php'""".stripMargin)
    .show(10)

  sparkSession
    .sql(
      """select *
        |from so_tags
        |where tag like 's%'""".stripMargin)
    .show(10)
  sparkSession
    .sql(
      """select *
        |from so_tags
        |where tag like 's%'
        |and (id = 25 or id = 108)""".stripMargin)
    .show(10)
  sparkSession
    .sql(
      """select *
        |from so_tags
        |where id in (25, 108)""".stripMargin)
    .show(10)

  sparkSession
    .sql(
      """select tag, count(*) as count
        |from so_tags group by tag""".stripMargin)
    .show(10)
  sparkSession
    .sql(
      """select tag, count(*) as count
        |from so_tags group by tag having count > 5""".stripMargin)
    .show(10)
  sparkSession
    .sql(
      """select tag, count(*) as count
        |from so_tags group by tag having count > 5 order by tag""".stripMargin)
    .show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("data//questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()

  // register temp table
  dfQuestionsSubset.createOrReplaceTempView("so_questions")

  sparkSession
    .sql(
      """select t.*, q.*
        |from so_questions q
        |inner join so_tags t
        |on t.id = q.id""".stripMargin)
    .show(10)

  sparkSession
    .sql(
      """select t.*, q.*
        |from so_questions q
        |left outer join so_tags t
        |on t.id = q.id""".stripMargin)
    .show(10)

  sparkSession
    .sql(
      """select t.*, q.*
        |from so_tags t
        |right outer join so_questions q
        |on t.id = q.id""".stripMargin)
    .show(10)
  sparkSession
    .sql("""select distinct tag from so_tags""".stripMargin)
    .show(10)


  def prefixStackoverflow(s: String): String = s"so_$s"

  sparkSession
    .udf
    .register("prefix_so", prefixStackoverflow _)
  sparkSession
    .sql("""select id, prefix_so(tag) from so_tags""".stripMargin)
    .show(10)





}
