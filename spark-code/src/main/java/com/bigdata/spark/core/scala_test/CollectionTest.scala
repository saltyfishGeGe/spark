package com.bigdata.spark.core.scala_test

object CollectionTest {

  def main(args: Array[String]): Unit = {

    val datas = List(1,3,5,9,7,6)

    val re: List[Int] = datas.sorted.reverse.take(3)

    val re2: List[Int] = datas.sortBy(k=>k)(Ordering.Int.reverse)

    println(re2)
  }

}
