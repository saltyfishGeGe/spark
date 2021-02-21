package com.bigdata.spark.core.scala_test

object ListTest {

  def main(args: Array[String]): Unit = {
    val datas = List(1,2,3,4,5)

    // 去掉集合中第一个值
    println(datas.tail)

    // 去掉集合中最后一个值
    println(datas.init)

    // 窗口推移, 类似拉链
    // iter(List(1, 2),List(2, 3),List(3, 4),List(4, 5))
    datas.sliding(2).foreach(println)
  }

}
