package spark

object List2BAG {
  def main(args: Array[String]) {
    val endLimit = args(0).toInt;
    val list = for(i <- 2 to endLimit by 2) yield List(i,i+1)
    println(list.toList)
  }

}
