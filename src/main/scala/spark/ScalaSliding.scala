package spark

object ScalaSliding {
  def main(args:Array[String]): Unit ={
    def inQ(num:Int):List[List[Int]]={
      val list =(1 to num).toList.map(_+1)
      list.sliding(2,2).toList
    }
    print(inQ(6))
  }

}
