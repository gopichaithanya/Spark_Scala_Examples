package spark

import scala.annotation.tailrec

object Recursion extends App {

  def factorial(n: Int): Int =
    if (n <= 1) 1
    else {
      println("Computing " + n + "First need factorial" + (n - 1))
      val result = n * factorial(n - 1)
      println("Computed factorial " + n)
      result
    }

  println(factorial(10))


  def anotherfact(n: Int): BigInt = {
    def factHelper(x: Int, accumulator: BigInt): BigInt =
      if (x <= 1) accumulator
      else factHelper(x - 1, x * accumulator)

    factHelper(n, 1)

  }

  println(anotherfact(5000))

  def concatTail(aString: String, n: Int, accumulator: String): String =
    if (n <= 0) accumulator
    else concatTail(aString, n - 1, aString + accumulator)

  println(concatTail("hello", 3, ""))

  def GCD(n: Int, m: Int): Int = {
    @tailrec
    def gcd(x: Int, y: Int): Int = {
      if (y == 0) x
      else gcd(y, x % y)
    }

    gcd(n, m)

}
  println(GCD(12,18))

}