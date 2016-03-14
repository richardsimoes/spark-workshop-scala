package com.datasignal.workshop

import org.apache.spark.SparkContext
import org.apache.spark.streaming._

/**
  * Created by rsimoes on 3/11/16.
  */
object SimpleStreaming extends App {

  //val sc = new SparkContext("local", "Simple Streaming App")
  //val sc = new SparkContext("local[2]", "Simple Streaming App")


  def addLengthToState(values: Seq[Int], state: Option[Int]) = {
    Some(values.sum + state.getOrElse(0))
  }


  def createSparkStreamingContext() = {
    println("Creating a brand new context...")
    val sc = new SparkContext("local[2]", "Simple Streaming App")
    val context = new StreamingContext(sc, Seconds(2))
    context.checkpoint("check")

    val stream = context.socketTextStream("localhost", 9999)
    stream
      .map(sentence => (sentence, sentence.length))
      .updateStateByKey(addLengthToState)
      .print()

    context
  }

  //val ssc = new StreamingContext(sc, Seconds(2))
  //ssc.checkpoint("checkpoint_dir")

  val ssc = StreamingContext.getOrCreate("check", createSparkStreamingContext)

  val stream = ssc.socketTextStream("localhost", 9999)

  //stream
  //  .map(sentence => (sentence, sentence.length))
  //  .print()

  stream
    .map(sentence => (sentence, sentence.length))
    .updateStateByKey(addLengthToState)
    .print()

  //stream.print()

  ssc.start()
  ssc.awaitTermination()

}
