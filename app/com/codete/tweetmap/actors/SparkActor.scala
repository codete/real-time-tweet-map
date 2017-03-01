package com.codete.tweetmap.actors

import akka.actor.{Actor, Props}
import com.codete.tweetmap.utils.{AppProperties, GeolocationEstimator, TwitterGeoInputDStream}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import play.libs.Akka
import twitter4j.FilterQuery


/**
  * Akka actor responsible for SparkContext initialization and Twitter streaming data gathering.
  */
class SparkActor extends Actor {

  /**
    * Runs data gathering when SparkGatheringJob with configuration message received.
    */
  override def receive = {
    case SparkGatheringJob(locations: Array[Array[Double]]) => {
      val sparkConf = new SparkConf().setAppName(AppProperties.APP_NAME).setMaster(AppProperties.SPARK_LOCAL_URL)
      val sparkStreamingContext = new StreamingContext(sparkConf, AppProperties.SPARK_BATCH_DURATION)

      registerSparkJobInContext(sparkStreamingContext, locations)

      sparkStreamingContext.start()
      sparkStreamingContext.awaitTermination()
    }
  }

  /**
    * Registers gathering job in the SparkStreamingContext.
    *
    * Creates tweet filter based on the provided location, processes tweets from the Twitter Streaming API,
    * converts them into Messages and send to MapActor.
    *
    * @param sparkStreamingContext initialized SparkStreamingContext
    * @param locations             coordinates of locations to be tracked
    */
  private def registerSparkJobInContext(sparkStreamingContext: StreamingContext, locations: Array[Array[Double]]): Unit = {
    // Creates a "clean" Twitter stream
    //    val stream = TwitterUtils.createStream(sparkStreamingContext, None, Nil)

    // Creates a Twitter stream pre-filtered by geolocation
    val geoFilter = createGeoFilterQuery(locations)
    val stream = new TwitterGeoInputDStream(sparkStreamingContext, Option(geoFilter))

    stream.foreachRDD(rdd => {
      val messages = rdd
        .filter(tweet => tweet.getGeoLocation != null || tweet.getPlace != null)
        .map(tweet => Message(tweet.getText, GeolocationEstimator.estimateTweetGeolocation(tweet)))
        .collect()
      if (messages.nonEmpty) {
        MapActor.map ! MessagePackage(AppProperties.APP_NAME, messages)
      }
    })
  }

  /**
    * Creates a new instance of FilterQuery and configures it to filter tweets by geolocation.
    *
    * @param locations coordinates of the bounding box
    * @return FilterQuery instance
    */
  private def createGeoFilterQuery(locations: Array[Array[Double]]): FilterQuery = {
    new FilterQuery().locations(locations(0), locations(1))
  }

}

object SparkActor {
  lazy val actor = Akka.system().actorOf(Props[SparkActor])

  def apply() = actor
}

case class SparkGatheringJob(locations: Array[Array[Double]])
