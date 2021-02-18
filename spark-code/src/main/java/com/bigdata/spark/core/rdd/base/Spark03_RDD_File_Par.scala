package com.bigdata.spark.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark03_rdd_file")

    val sc : SparkContext = new SparkContext(sparkConf)


    // spark采用hadoop读取文件的方式，采用一行一行的读
    // 又进行偏移量计算，
    // data4.txt内容如下：?为换行字节
    //0123456？？
    //789？？
    //0？
    // data4.txt有15个字节，按4个分区读取， 15 % 4 = 3 ， 3/4 > 10%会新增多一个新的分区。共五个分区文件。每个分区的范围是[0-3]
    // 则第一个文件是0123456 (读取一整行[0-3])
    // 第二个文件无数据[3,6]，因为读取的偏移量范围内的数据已被第一个文件读取完
    // 第三个文件,[6,9],读取到数字7，则整行读取
    // 第四个文件,[9,12],已被第三个文件读取完，为空
    // 第五个文件读取最后一行
    val rdd: RDD[String] = sc.textFile("datas/data4.txt", 4)

    rdd.saveAsTextFile("output")

    sc.stop
  }

}
