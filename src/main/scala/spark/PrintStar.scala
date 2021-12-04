package spark

object PrintStar {
  def main(args: Array[String]): Unit ={
    def print_st1(n:Int):Unit={
      if(n==0) return
      println("*" *n)
      print_st1(n-1)

    }
    print(print_st1(5))
  }


}
