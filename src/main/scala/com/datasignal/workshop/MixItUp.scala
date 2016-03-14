package com.datasignal.workshop

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

/**
  * Created by rsimoes on 3/11/16.
  */
object MixItUp extends App {
  /*
    def compute(input: RDD[String]) = input
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    def featurization(input: RDD[String]) : RDD[LabeledPoint] = ??

    val sc = new SparkContext("local[2]", "Simple Streaming App")
    val sqlContext = new SQLContext(sc)

    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("check")

    val stream = ssc.socketTextStream("localhost", 9999)

    stream
        .window(Seconds(30), Seconds(10))
      .transform(rdd => compute(rdd))
      .print()

    //MLlib
    stream
      .window(Seconds(30), Seconds(10))
      .transform(rdd => {
        val model = SVMWithSGD.train(featurization(rdd), 12)
        rdd.map(input => (input, model.predict()))
      })
      .print()

    ssc.start()
    ssc.awaitTermination()
  */
}
