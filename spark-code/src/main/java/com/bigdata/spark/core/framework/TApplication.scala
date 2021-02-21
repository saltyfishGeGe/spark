package com.bigdata.spark.core.framework

import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def start(master: String = "local[*]", appName: String = "application")(op: => Unit){
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)

    EnvUtils.put(sc)

    try{
      op
    } catch {
      case ex : Throwable => println(ex.getMessage)
    }

    sc.stop()
    EnvUtils.clear
  }

}
