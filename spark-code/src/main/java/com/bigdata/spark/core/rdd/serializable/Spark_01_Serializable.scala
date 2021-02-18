package com.bigdata.spark.core.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_Serializable {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("serializable")

    val sc = new SparkContext(sparkConf)

    //3.创建一个 RDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    //3.1 创建一个 Search 对象
    val search = new Search("hello")

    //3.2 函数传递，打印：ERROR Task not serializable
    search.getMatch1(rdd).collect().foreach(println)

    //3.3 属性传递，打印：ERROR Task not serializable
    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }

  // 类的构造参数是类的属性，构造参数需要进行闭包检测
  //
  class Search(query:String) extends Serializable {

    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }
  }
}
