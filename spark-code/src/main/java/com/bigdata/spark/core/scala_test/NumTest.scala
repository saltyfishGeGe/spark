package com.bigdata.spark.core.scala_test

class NumTest {

  val datas = List(1, 2, 3, 4)

  val logic = (num: Int) => {num * 2}

  // 定义输入值和返回值类型
  val logic2 : (Int)=>(Int) = _*2

  val test3 = (aa:String, bb: String) => {
    aa.concat(bb)
  }

  def test4 : ((String,String) => String) = {
    _.concat(_)
  }

  // 计算
  def compute(): Unit = {
    val results: List[Int] = datas.map(logic2)
    return results
  }


  def testSt(fn:(String, String) => String): Unit = {
      val str: String = fn("aaa","bbb")
      println(str)
  }

}
