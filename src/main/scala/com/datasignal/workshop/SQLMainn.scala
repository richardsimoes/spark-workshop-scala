/*
 * Created by RSimoes
 */

package com.datasignal.workshop

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object SQLMainn extends App {
  val sc = new SparkContext(new SparkConf().setAppName("SQL Mainn").setMaster("local"))

  val sqlContext = new SQLContext(sc)


  val rdd: RDD[Row] = sc.parallelize(List(("Cortazar", 10), ("Gabo", 15), ("William", 14)))
    .map { case (name, count) => Row(name, count) }

  // Simple
  /*
  val schema = StructType(List(
    StructField("name", StringType)
  ))
*/
  // Not so simple
  val schema = StructType(List(
    StructField("name", StringType, nullable = false),
    StructField("value", IntegerType, nullable = false)
  ))

  val df = sqlContext.createDataFrame(rdd, schema)
  df.printSchema()
  df.registerTempTable("authors")

  // Simple query
  /*
  val authorsDF = sqlContext.sql(
    """
      |SELECT * FROM authors
    """.stripMargin)
  */
  // Not so simple query

  val authorsDF = sqlContext.sql(
    """
      |SELECT * FROM authors WHERE value > 10
    """.stripMargin)

  //authorsDF.show()
  authorsDF.save("authors.parquet")

}