package spark

object scalaExc {
  def main(args:Array[String]): Unit ={
    val mylist =List(7,8,9,10)
    val result_pair= mylist.map(x => x+1)
    print(result_pair)
  }

}
