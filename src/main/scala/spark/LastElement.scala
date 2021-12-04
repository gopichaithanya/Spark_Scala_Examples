package spark

 import scala.annotation.tailrec

  object LastElement {
    def main(args: Array[String]): Unit = {
      val numList = List(11, 22, 33, 44, 55)
      print(findLast(numList))
    }

    val findLast = (list: List[Int]) => {
      @tailrec
      def lastElement(list: List[Int]): Int = {
        if (list == null || list.isEmpty) {
          0
        }
        else {
          if (list.length == 1) {
            list(0)
          }
          else {
            lastElement(list.tail)
          }
        }
      }

      lastElement(list.tail)
    }
  }


