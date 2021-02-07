package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_operator_sample {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5,6,7,8,9,10)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    // 1.第一个参数，是否可以重复多次抽取
    // 2.每条数据抽取到的概率
    //    不放回：基准值，为RDD设定一个样本的基准值，值越大则样本越大
    //    放回：设定每个数据被抽取的可能次数
    // 3.随机数种子（不传使用默认Utils.random.nextLong）
    // 该算子可用于数据倾斜
    println(rdd.sample(false, 0.5).collect().mkString(","))

    sc.stop
  }

}