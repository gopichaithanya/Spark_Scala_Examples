import org.scalatest.flatspec.AnyFlatSpec
import spark._
class MathUtilsTest extends AnyFlatSpec{

  it should "match" in {
    assert(10 == MathUtils.addVars(5,5))
  }
  it should "match1" in {
    assert(20 == MathUtils.multiplyVars(10,2))
  }



}
