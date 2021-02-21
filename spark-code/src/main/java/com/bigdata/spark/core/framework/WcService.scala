package com.bigdata.spark.core.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WcService {

  def execute: Unit ={

    val sc: SparkContext = EnvUtils.take()

    val lines: RDD[String] = sc.textFile("datas/wc.txt")

    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(w => w)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val arr: Array[(String, Int)] = wordToCount.collect()
    arr.foreach(println)
  }

}
