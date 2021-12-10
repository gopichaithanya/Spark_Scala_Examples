package spark

object scalaExc {
  def main(args:Array[String]): Unit ={
    val my_list =List(7,8,9,10)
    val result_pair= my_list.map(x =>x+1)
    print(result_pair)
  }

}
