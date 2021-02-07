package com.bigdata.spark.core.operator

import org.apache.hadoop.mapred.Partitioner
import org.apache.hadoop.mapred.lib.HashPartitioner
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark13_operator_partitionBy {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5,6,7)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    val mapRDD: RDD[(Int, Int)] = rdd.map((_,1))

    // partitionBy需要k,v键值对RDD才可调用到
    // RDD => PairRDDFunction
    // 隐式转换 RDD.scala中存在implicit修饰的隐式函数：rddToPairRDDFunctions

    // 直接分区规则进行重分区
    mapRDD.partitionBy(new spark.HashPartitioner(3)).saveAsTextFile("output")

    sc.stop
  }

}