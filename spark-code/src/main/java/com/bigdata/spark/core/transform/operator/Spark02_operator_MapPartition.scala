package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_operator_MapPartition {

  def main(args: Array[String]): Unit = {


    val datas: List[Int] = List[Int](1,2,3,4)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator_MapPartition")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    // 方法逻辑存在{}， 且直接在map中通过匿名函数传入会抛出序列化异常。在此情况下scala不会自动序列化
    def filterFunc(num:Int): Boolean = {
      if(num == 2 || num == 4){
        return true
      }
      return false
    }

    // 性能高，把分区内数据全部拿到后在执行
    /**
      * map算子：分区内数据逐一执行，类似串行操作，占用内存低
      * mapPartitions算子：以分区为单位进行批处理，速度更快，占用内存较高
      */
    val mapRDD: RDD[Int] = rdd.mapPartitions(
      // iter是一个分区完整的数据，参数为迭代器，返回值也为迭代器，可以在方法中进行数据清洗，最后返回值为迭代器即可
      // 在内存较小，数据量较大的场景慎用该算子，因为会持有一个分区所有数据的引用，当数据未处理完时，已处理的数据内存并未释放。可能导致内存溢出
      iter => {
        println(">>>>>>")
        // 数据过滤
//        val filterIter: Iterator[Int] = iter.filter(filterFunc)
        val filterIter = iter.filter(num => {
          num == 2 || num == 4
        })

        filterIter.map(_ * 2)
      }
    )

    mapRDD.foreach(println)

    mapRDD.collect()

    sc.stop
  }

}
