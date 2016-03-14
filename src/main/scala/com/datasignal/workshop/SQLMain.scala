/*
 * Created by RSimoes
 */

package com.datasignal.workshop

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

case class Author(name: String, nBooks: Int)

object SQLMain extends App {
  val sc = new SparkContext(new SparkConf().setAppName("SQL Main").setMaster("local"))

  val sqlContext = new SQLContext(sc)

  val rdd = sc.parallelize(List(Author("Cortazar", 10), Author("Gabo", 15), Author("William", 14)))

  import sqlContext.implicits._

  // Schema inferred
  // rdd.toDF().show()

  // Override the column names
  // rdd.toDF("name", "count").show()

  // Print Schema
  val df = rdd.toDF("name", "count")
  df.printSchema()

}