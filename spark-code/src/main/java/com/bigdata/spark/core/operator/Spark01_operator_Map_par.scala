package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_operator_Map_par {

  def main(args: Array[String]): Unit = {


    val datas: List[Int] = List[Int](1,2,3,4)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator_Map")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 1)

    val mapRDD : RDD[Int] = rdd.map( num => {
      println(">>>>>>" + num)
      num
    })

    val mapRDD2: RDD[Int] = mapRDD.map(num => {
      println("#######" + num)
      num
    })

    //>>>>>>1
    //#######1
    //>>>>>>2
    //#######2
    //>>>>>>3
    //#######3
    //>>>>>>4
    //#######4
    // rdd计算逻辑是分区内数据顺序执行，只有一条数据执行完所有rdd逻辑后才会执行下一条
    // 多个分区并行执行，分区内数据执行是有序的
    // 不同分区之间的数据之间是无序的
    mapRDD2.collect()

    sc.stop
  }

}
