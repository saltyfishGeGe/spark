package com.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    // application
    // spark framework

    // TODO 建立spark框架连接

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    // 被val声明的对象不可以改变地址， var为可变
    val sc = new SparkContext(conf)


    // TODO 执行业务操作

    // 1. 读取文件
    // sc.addFile("/datas/wc.txt")
    val lines: RDD[String] = sc.textFile("datas")

    // 2. 分词
    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    // 3. 统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(w => w)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 4. 打印结果
    val arr: Array[(String, Int)] = wordToCount.collect()
    arr.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }

}
