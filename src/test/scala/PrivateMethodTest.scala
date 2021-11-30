import org.apache.spark.sql.DataFrame
import org.scalatest.GivenWhenThen
import org.scalatest._
import spark._

class PrivateMethodTest  extends FeatureSpec with GivenWhenThen with  SparkSupportMixin with PrivateMethodTester {
  val enrichColumn = PrivateMethod[DataFrame]('privateMethod)
  val classToTest = new ClassToTestWithPrivateMethod
  val sparkDataLoader = new SparkSampleData
  val dataFrameFromClass = classToTest invokePrivate enrichColumn(
    sparkDataLoader.getSampleDataFrame(sparkSession = sparkSession),
    "new_column_to_add",
    "value for newly added column"
  )

  assert(dataFrameFromClass.columns.contains("new_column_to_add"))
  //assert(dataFrameFromObject.columns.contains("new_column_to_add"))



}
