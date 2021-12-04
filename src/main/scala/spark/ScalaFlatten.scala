package spark

import scala.None.flatten

object ScalaFlatten {
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3, List(4, 5, 6, List(8, 9, 10)))

    def flatten(l: List[Any]): List[Any] = l match {
      case Nil => Nil
      case (h: List[_]) :: tail => flatten(h) ::: flatten(tail)
      case h :: tail => h :: flatten(tail)
    }
    println(flatten(list1))
  }
}

