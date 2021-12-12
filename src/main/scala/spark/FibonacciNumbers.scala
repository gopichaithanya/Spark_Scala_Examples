package spark

import scala.annotation.tailrec

object FibonacciNumbers extends App {

  def isPrime(n:Int):Boolean ={
    @tailrec
    def isPrimeUtils(t:Int):Boolean = {
      if(t==2) true
      else (n%t !=0 && isPrimeUtils(t-1))
    }
    isPrimeUtils(n/2)
  }
  println(isPrime(19))

}