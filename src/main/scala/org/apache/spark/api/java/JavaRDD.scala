package org.apache.spark.api.java

import java.io.Serializable
import java.util
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class JavaRDD[T] {

  var lines:ArrayBuffer[T] = new ArrayBuffer[T]

  def flatMap[U](f: FlatMapFunction[T, U]): JavaRDD[U] = {
    var ret = new JavaRDD[U]
    for (line <- lines){
      for (item <- f.call(line).asScala)
        ret.lines += item
    }
    ret
  }

  def map[R](f: Function[T, R]): JavaRDD[R] = {
    var ret = new JavaRDD[R]
    for (line <- lines)
        ret.lines += f.call(line)
    ret
  }

  def reduce(f: Function2[T, T, T]):T = {
    var first = lines.apply(0)
    lines.remove(0)
    for (line <- lines)
      first = f.call(first,line)
    first
  }
}

trait FlatMapFunction[T, R] extends Serializable {
  @throws[Exception]
  def call(t: T): java.util.Iterator[R]
}

trait Function[T1, R] extends Serializable {
  @throws[Exception]
  def call(v1: T1): R
}

trait Function2[T1, T2, R] extends Serializable {
  @throws[Exception]
  def call(v1: T1, v2: T2): R
}
