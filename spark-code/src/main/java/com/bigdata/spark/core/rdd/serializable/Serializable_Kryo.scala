package com.bigdata.spark.core.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 开始其他序列化方式
  */

object Serializable_Kryo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用 kryo 序列化的自定义类
      .registerKryoClasses(Array(classOf[Searcher]))

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val searcher = new Searcher("hello")

    val result: RDD[String] = searcher.getMatchedRDD1(rdd)

    result.collect.foreach(println)
  }


  case class Searcher(val query: String) {

    def isMatch(s: String) = {
      s.contains(query)
    }

    def getMatchedRDD1(rdd: RDD[String]) = {
      rdd.filter(isMatch)
    }

    def getMatchedRDD2(rdd: RDD[String]) = {
      val q = query
      rdd.filter(_.contains(q))
    }

  }

}