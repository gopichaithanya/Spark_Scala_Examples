package spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object ObjectToTestWithPrivateMethod {
  private val privateField: String = "private field"
  private def privateMethod(dataFrame: DataFrame,
                            columnToAdd: String,
                            value: String) = {
    dataFrame.withColumn(columnToAdd, lit(value))
  }

}
