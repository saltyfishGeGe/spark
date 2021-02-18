package com.bigdata.spark.core.rdd.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {

  def main(args: Array[String]): Unit = {

    // application
    // spark framework

    // TODO 建立spark框架连接

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    // 被val声明的对象不可以改变地址， var为可变
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    // 直接通过key进行聚合
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey {
      (r1, r2) => {
        r1 + r2
      }
    }

    // _ 函数编程至简原则
    // 因为Scala是函数式编程，函数可以在Scala中以参数、返回值和变量等形式和位置使用。所以函数至简原则显得至关重要，总的来说，函数至简原则就是能省则省，有人说“你那不废话吗”，重点是怎么简？在哪里可以简？下面一一讲明：
    //（1）return可以省略，Scala会使用函数体的最后一行代码作为返回值
    //（2）如果函数体只有一行代码，可以省略花括号
    //（3）返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    //（4）如果有return，则不能省略返回值类型，必须指定
    //（5）如果函数明确声明unit，那么即使函数体中使用return关键字也不起作用
    //（6）Scala如果期望是无返回值类型，可以省略等号
    //（7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    //（8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    //（9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略

    // val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    // ./spark-shell linux下直接执行spark代码
    // sc.textFile("/root/data.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect()

    // 4. 打印结果
    val arr: Array[(String, Int)] = wordToCount.collect()
    arr.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }

}
