package com.bigdata.spark.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 并行
object Spark01_RDD_Memory_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_01")

    val sc : SparkContext = new SparkContext(sparkConf)

     val list: Seq[Int] = Seq(1,2,3,4)

    // numSlices, 分区数量
    // 不传值时使用sparkConf中spark.default.parallelism
    // 若取不到则拿totalCores,这个值属于运行环境最大可用核数
    val rdd = sc.makeRDD(list,2)

    // 读取文件也可设置分区数量，取defaultParallelism和2的最小值
//    val rdd: RDD[String] = sc.textFile("datas",3)

    // 将分区文件保存在本地
    rdd.saveAsTextFile("output")

    sc.stop
  }

}
