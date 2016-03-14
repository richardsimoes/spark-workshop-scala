package com.datasignal.workshop

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by rsimoes on 3/11/16.
  */
object SQLload extends App {
  val sc = new SparkContext("local", "SQL Load")

  val sqlContext = new SQLContext(sc)
  val df = sqlContext.load("authors.parquet")

  //df.save("authors.json", "json")

  df.show()
  df.printSchema()
}
