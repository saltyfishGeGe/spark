package com.bigdata.spark.core.framework


object WordCountController extends App with TApplication with TestApplication {

  val wcService = new WcService

  start("local[*]", "wc"){
    println("aa:" + aa)
    wcService.execute
  }

}
