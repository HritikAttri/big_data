package com.spark.examples

import scala.io.Source
import org.apache.log4j.{Level, Logger} 
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

object spark_streaming {
  def setupLogging() = {  
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  def setupTwitter() = {
    for(line <- Source.fromFile("C://twitter.txt").getLines) {
      val fields = line.split(" ")
      if(fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  def main(args: Array[String]) {
    
    // Setup the twitter configuration
    setupTwitter()
    
    // Creating a streaming context which will stream batches of data in every 1 second
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    // Set log level to print errors only
    setupLogging()
    
    // Create a DStream from twitter
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Extract text from tweets
    val text = tweets.map(x => x.getText())
    
    // Extract each word from text
    val words = text.flatMap(x => x.split(" "))
    
    // Keep the words that start with a hashtag
    val hashtags = words.filter(x => x.startsWith("#"))
    
    val hashtags_values = hashtags.map(x => (x, 1))
    
    // Count them over a 5 minute window every 1 second
    val hashtags_count = hashtags_values.reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1))
    
    // Sort the result by hashtag count in descending order
    val results = hashtags_count.transform(x => x.sortBy(x => x._2, false))
    
    // Print top 10 results
    results.print
    
    // Set up a checkpoint 
    ssc.checkpoint("C://checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}