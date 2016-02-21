/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.datasignal.workshop

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object  DataLoad {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Data Load"))
    println("num lines: " + countLines(sc, args(0)))
  }

  def countLines(sc: SparkContext, path: String): Long = {
    sc.textFile(path).count()
  }
}

