package com.datasignal.workshop

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by rsimoes on 3/11/16.
  */
object SQLMainnn extends App {

  val sc = new SparkContext("local", "SQL Main")

  val sqlContext = new SQLContext(sc)
  val path = "/Users/rsimoes/dev/hdp-dev/titanicData/data_titanic.json"
  val df = sqlContext.load(path, "json")

  df.registerTempTable("passengers")

  // Query
  /*
  sqlContext.sql(
    """
      |SELECT sex
      | , (SUM(age) / count(*)) AS mean_age
      | , MIN(age) as youngest
      | , MAX(age) as oldest
      | FROM passengers GROUP BY sex
    """.stripMargin).show()
*/
  //df.printSchema()
  //df.show()

  //Caching
  sqlContext.cacheTable("passengers")

  //UDF
  sqlContext.udf.register("first_letter",
    (input: String) => input.charAt(0).toString())

  sqlContext.sql("SELECT first_letter(name) from passengers").show()
}
