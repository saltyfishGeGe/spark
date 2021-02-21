package com.bigdata.spark.core.framework

import org.apache.spark.SparkContext

object EnvUtils {

  private val threadLocal = new ThreadLocal[SparkContext]

  def take(): SparkContext = {
    threadLocal.get()
  }

  def put(sc: SparkContext): Unit = {
    threadLocal.set(sc)
  }

  def clear : Unit = {
    threadLocal.remove()
  }
}
