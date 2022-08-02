package org.apache.spark.sql

import org.apache.spark.api.java.JavaRDD

import scala.io.Source

object SparkSession{

  class Builder  {
    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    def config(key:String,value:String):Builder={
      options += key -> value;
      this
    }

    def appName(name: String):Builder= config("spark.app.name",name)

    def master(master: String): Builder = config("spark.master", master)

    def getOrCreate(): SparkSession = synchronized {
      new SparkSession()
    }
  }

  def builder(): Builder = new Builder
}

class SparkSession {
  def read():SparkSession = this

  var rdd:JavaRDD[String] = null

  def textFile(filePath:String) :SparkSession = {
    val reader = Source.fromFile(filePath);

    rdd = new JavaRDD[String]
    for (line <- reader.getLines()) {
      rdd.lines += line
    }
    this
  }

  def javaRDD():JavaRDD[String]={
    rdd
  }

  def stop()={
    println("stoped")
  }
}
