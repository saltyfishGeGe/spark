package com.bigdata.spark.core.rdd.depend

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("datas")
    println(lines.toDebugString)
    println("*******************************")

    val words: RDD[String] = lines.flatMap(line => line.split(" "))
    println(words.toDebugString)
    println("*******************************")

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(w => w)
    println(wordGroup.toDebugString)
    println("*******************************")

    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    println(wordToCount.toDebugString)
    println("*******************************")

    val arr: Array[(String, Int)] = wordToCount.collect()
    arr.foreach(println)

    sc.stop()
  }

}
