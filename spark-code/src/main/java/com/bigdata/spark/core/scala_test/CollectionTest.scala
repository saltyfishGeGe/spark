package com.bigdata.spark.core.scala_test

object CollectionTest {

  def main(args: Array[String]): Unit = {

    val datas = List(1,3,5,9,7,6)

    val datas2 = List(("a", 1), ("c", 3), ("b", 9))

    val re: List[Int] = datas.sorted.reverse.take(3)

    val re2: List[Int] = datas.sortBy(k=>k)(Ordering.Int.reverse)

    val tuples: List[(String, Int)] = datas2.sortBy(k => k._1)(Ordering.String.on(k=>k))

    println(tuples)
  }

}
