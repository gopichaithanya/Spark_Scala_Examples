package spark

import scala.annotation.tailrec

object ScalaFunctions extends App {

  def addfun(a: String, b: Int): String = {
    a + " " + b
  }

  println(addfun("hello", 3))

  def repeatedFunction(aString: String, n: Int): String = {
    if (n == 1) aString
    else aString + repeatedFunction(aString, n - 1)
  }

  println(repeatedFunction("hello", 3))

  def aBigFunction(n: Int): Int = {
    def asmallFunction(a: Int, b: Int): Int = a + b

    asmallFunction(n, n - 1)

  }

  def factorial(n: Int): Int =
    if (n <= 0) 1
    else n * factorial(n - 1)

  println(factorial(5))

  def fibonacci(n: Int): Int =
    if (n <= 2) 1
    else fibonacci(n - 1) + fibonacci(n - 2)

  println(fibonacci(8))

  def isPrime(n: Int): Boolean = {
    @tailrec
    def isPrimeUntil(t: Int): Boolean =
      if (t <= 1) true
      else n % t != 0 && isPrimeUntil(t - 1)
      isPrimeUntil(n / 2)
    }
  println(isPrime(21*7))

}


