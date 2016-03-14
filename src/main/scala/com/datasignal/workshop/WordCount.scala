/*
 * Created by RSimoes
 */

package com.datasignal.workshop

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Word Count").setMaster("local"))
    countWords(sc, args(0))
  }

  def countWords(sc: SparkContext, path: String) = {
    val f = sc.textFile(path)
    val wc = f
      .flatMap(l => l.split(" "))
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile("./wcresults")
  }
}

