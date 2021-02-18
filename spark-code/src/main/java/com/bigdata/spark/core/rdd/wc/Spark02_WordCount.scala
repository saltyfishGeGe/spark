package com.bigdata.spark.core.rdd.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

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

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(w => w._1)

    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    // 4. 打印结果
    val arr: Array[(String, Int)] = wordToCount.collect()
    arr.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }

}
