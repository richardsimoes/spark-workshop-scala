/*
 * Created by RSimoes
 */

package com.datasignal.workshop

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object  DataLoad {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Data Load").setMaster("local"))
    println("num lines: " + countLines(sc, args(0)))
  }

  def countLines(sc: SparkContext, path: String): Long = {
    sc.textFile(path).count()
  }
}

