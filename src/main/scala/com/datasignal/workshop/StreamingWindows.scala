package com.datasignal.workshop

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

/**
  * Created by rsimoes on 3/11/16.
  */
object StreamingWindows extends App {


  def compute(input: RDD[String]) = input
    .flatMap(_.split(" "))
    .map((_, 1))
    .reduceByKey(_ + _)

  val sc = new SparkContext("local[2]", "Simple Streaming App")

  val ssc = new StreamingContext(sc, Seconds(2))
  ssc.checkpoint("check")

  val stream = ssc.socketTextStream("localhost", 9999)

  /*
  stream
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKeyAndWindow((a:Int, b:Int) => a + b, Seconds(30), Seconds(10))
      .print()
  */

  //Reusing Computation (Wont work)
  //stream
  //  .foreachRDD(rdd =>
  //    compute(rdd).saveAsTextFile(path)
  //  )

  stream
    .window(Seconds(30), Seconds(10))
    .transform(rdd => compute(rdd))
    .print()

  ssc.start()
  ssc.awaitTermination()

}
