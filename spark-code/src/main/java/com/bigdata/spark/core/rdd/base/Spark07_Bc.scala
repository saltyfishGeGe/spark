package com.bigdata.spark.core.rdd.base

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark07_Bc {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_rdd")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",1),("c",1)))

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",2),("b",2),("c",3)))

    val mapData: mutable.Map[String, Int] = mutable.Map(("a",2),("b",2),("c",3))

    // join会导致数据量指数增长，shuffle数据量过大，性能较低
    // 可以理解为结构转换
    //(a,(1,2))
    //(b,(1,2))
    //(c,(1,3))
    //val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    // joinRDD.collect().foreach(println)

    // 与join操作结果相同，但效率更高
    //(a,(1,2))
    //(b,(1,2))
    //(c,(1,3))
    // 这里的map数据源作为闭包的数据，闭包数据都是以task作为单位发送的。在每个task中都存在一份数据(理解成一个静态数据传递到各个task中)
    // 这样可能导致一个executor中存在大量重复的数据
//    rdd1.map(t => {
//      val key = t._1
//      val value = t._2
//      val l: Int = mapData.getOrElse(key, 0)
//      (key, (value, l))
//    }).collect().foreach(println)

    /**
      * Executor是一个JVM，优化方式就是让同一个executor中的所有task共享同一份数据，达到优化的目的
      * spark中的广播变量，就是将闭包数据保存在executor中，且广播变量无法修改，为共享只读变量
      */
    // 将数据封装进广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(mapData)

    rdd1.map(t => {
      val key = t._1
      val value = t._2
      // 从广播变量中获取闭包数据
      val l: Int = bc.value.getOrElse(key, 0)
      (key, (value, l))
    }).collect().foreach(println)

    sc.stop()
  }

}
