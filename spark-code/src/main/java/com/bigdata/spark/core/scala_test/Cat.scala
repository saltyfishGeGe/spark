package com.bigdata.spark.core.scala_test

/**
  * 伴生对象，伴生类
  * 对于scala的意义是：①弥补类中不能定义 static 属性的缺陷。②节省内存资源。③资源共享 （在JVM中单独一块内存存储静态属性）
  */
class Cat(name:String) {

  def sayHello(): Unit ={
    println(s" ${name} cat class say hello... and ${Cat.sound} and private ${Cat.sound2}")
    println()
  }

}

// scala里面没有 static 关键字
// 在同一个scala文件中定义一个类，同时定义一个同名的object，那么它们就是伴生类和伴生对象的关系，可以互相直接访问私有的field。
// 可以简单理解成：object用于实现所有的静态成员，class用于实现所有的实例成员
object Cat{

  val sound: String = "喵喵喵"

  // 私有变量在伴生类中同样可以获取
  private val sound2 = "miaomiaomiao"

  def sayHi: Unit ={
    println("cat object say hi...")
  }

  // 通过apply方法构造伴生类对象
  def apply(name: String): Cat = new Cat(name)


  def main(args: Array[String]): Unit = {

    val cat = new Cat("yzb")
    // 类方法，类实例可调用
    cat.sayHello
    // 类似java的静态方法。无法通过类实例调用
    Cat.sayHi
  }

}
