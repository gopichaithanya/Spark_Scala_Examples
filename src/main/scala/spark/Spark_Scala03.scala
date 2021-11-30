package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.util.Scanner


object Spark_Scala03  extends App{

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark=SparkSession.builder().appName("sparkApp01").master("local[3]").getOrCreate()
  import spark.implicits._
  case class Salary(depName: String, empNo: Long, name: String,
                    salary: Long, hobby: Seq[String])
  val empsalary = Seq(
    Salary("sales",     1,  "Alice",  5000, List("game",  "ski")),
    Salary("personnel", 2,  "Olivia", 3900, List("game",  "ski")),
    Salary("sales",     3,  "Ella",   4800, List("skate", "ski")),
    Salary("sales",     4,  "Ebba",   4800, List("game",  "ski")),
    Salary("personnel", 5,  "Lilly",  3500, List("climb", "ski")),
    Salary("develop",   7,  "Astrid", 4200, List("game",  "ski")),
    Salary("develop",   8,  "Saga",   6000, List("kajak", "ski")),
    Salary("develop",   9,  "Freja",  4500, List("game",  "kajak")),
    Salary("develop",   10, "Wilma",  5200, List("game",  "ski")),
    Salary("develop",   11, "Maja",   5200, List("game",  "farming"))).toDS
  empsalary.createTempView("empsalary")
  empsalary.show()
  val overCategory = Window.partitionBy('depName).orderBy('salary desc)
  val data_df= empsalary.withColumn("salaries", collect_list('salary) over overCategory)
    .withColumn("average_salary",(avg('salary) over overCategory).cast("int"))
    .withColumn("total_salary",sum('salary) over overCategory)
    .select("depName","empNo","name","salary","salaries","average_salary","total_salary")

  val df = empsalary.withColumn(
    "salaries", collect_list('salary) over overCategory).withColumn(
    "rank", rank() over overCategory).withColumn(
    "dense_rank", dense_rank() over overCategory).withColumn(
    "row_number", row_number() over overCategory).withColumn(
    "ntile", ntile(3) over overCategory).withColumn(
    "percent_rank", percent_rank() over overCategory).select(
    "depName", "empNo", "name", "salary", "rank", "dense_rank", "row_number", "ntile", "percent_rank")
  df.show(false)
  val top_df = empsalary.withColumn(
    "row_number", row_number() over overCategory).filter(
    'row_number <= 2).select(
    "depName", "empNo", "name", "salary")
  top_df.show(false)
  val l_df = empsalary.withColumn(
    "lead", lead('salary, 1).over(overCategory)).withColumn(
    "lag", lag('salary, 1).over(overCategory)).select(
    "depName", "empNo", "name", "salary", "lead", "lag")
  l_df.show(false)

  val diff = l_df.withColumn(
    "higher_than_next", 'salary - 'lead).withColumn(
    "lower_than_previous", 'lag - 'salary)
  diff.show()
  val null_diff = l_df.withColumn(
    "higher_than_next", when('lead.isNull, 0).otherwise('salary - 'lead)).withColumn(
    "lower_than_previous", when('lag.isNull, 0).otherwise('lag - 'salary))
  null_diff.show()
  null_diff.filter('higher_than_next > (lit(2) * 'salary)).show(false)

  data_df.show()

  val running_total = empsalary.withColumn(
    "rank", rank().over(overCategory)).withColumn(
    "costs", sum('salary).over(overCategory)).select(
    "depName", "empNo", "name", "salary", "rank", "costs")
  running_total.show(false)

  val overRowCategory = Window.partitionBy('depname).orderBy('row_number)

  val running_total1 = empsalary.withColumn(
    "row_number", row_number() over overCategory).withColumn(
    "costs", sum('salary) over overRowCategory).select(
    "depName", "empNo", "name", "salary", "row_number", "costs")
  running_total1.show(false)

  val overCategory2=Window.partitionBy('depName).rowsBetween(Window.currentRow,1)
  val range_df = empsalary.withColumn(
    "salaries", collect_list('salary) over overCategory2).withColumn(
    "total_salary", sum('salary) over overCategory2)
  range_df.select("depName", "empNo", "name", "salary", "salaries", "total_salary").show(false)
  val overCategory3 = Window.partitionBy('depName).orderBy('salary desc).rowsBetween(
    Window.currentRow, 1)
  val range_df1 = empsalary.withColumn(
    "salaries", collect_list('salary) over overCategory3).withColumn(
    "total_salary", sum('salary) over overCategory3)
  range_df1.select("depName", "empNo", "name", "salary", "salaries", "total_salary").show(false)

  val overCategory4 = Window.partitionBy('depName).orderBy('salary).rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing)

  val range_df2 = empsalary.withColumn(
    "salaries", collect_list('salary) over overCategory4).withColumn(
    "median_salary", element_at('salaries, (size('salaries)/2 + 1).cast("int")))
  range_df2.select("depName", "empNo", "name", "salary", "salaries", "median_salary").show(false)

  val dfMedian = empsalary.groupBy("depName").agg(
    sort_array(collect_list('salary)).as("salaries")).select(
    'depName, 'salaries, element_at('salaries, (size('salaries)/2 + 1).cast("int")).as("median_salary"))
  val range_df4 = empsalary.join(broadcast(dfMedian), "depName").select(
    "depName", "empNo", "name", "salary", "salaries", "median_salary")
  df.show(false)
  val sc=new Scanner(System.in)
  sc.nextLine()
  sc.close()

}
