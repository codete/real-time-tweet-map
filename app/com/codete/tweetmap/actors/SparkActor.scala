package com.codete.tweetmap.actors

import akka.actor.{Actor, Props}
import com.codete.tweetmap.controllers.Application
import com.codete.tweetmap.utils.TwitterGeoInputDStream
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import play.libs.Akka
import twitter4j.{FilterQuery, Status}

import scala.collection.mutable.ArrayBuffer

/**
  * Akka actor responsible for SparkContext initialization and Twitter streaming data gathering.
  */
class SparkActor extends Actor {

  /**
    * Runs data gathering when SparkGatheringJob with configuration message received.
    */
  override def receive = {
    case SparkGatheringJob(locations: Array[Array[Double]]) => {
      val sparkConf = new SparkConf().setAppName(Application.APP_NAME).setMaster(Application.SPARK_URL)
      val sparkStreamingContext = new StreamingContext(sparkConf, Application.SPARK_BATCH_DURATION)

      registerSparkJobInContext(sparkStreamingContext, locations)

      sparkStreamingContext.start()
      sparkStreamingContext.awaitTermination()
      /* TODO Akka - should SparkContext be a singleton ? Isin't it initialized on received message ?
         If it is this is really evil because it's a heavy object. Moreover we are using local spark distribution and it might raise up on every incoming tweet series.
         Please verify it
      */
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
    // TODO Can't we use implicit wrappers. Look like a plain old Java approach ? VERIFY!!!

    stream.foreachRDD(rdd => {
      var messages = new ArrayBuffer[Message]()
      rdd
        .filter(tweet => tweet.getGeoLocation != null || tweet.getPlace != null)
        .collect()
        // TODO Wouldn't it be better to use `map` and then collect instead of foreach and appending ?
        .foreach(tweet => {
          messages += new Message(tweet.getText, extractGeolocation(tweet))
        })
      if (messages.nonEmpty) {
        MapActor.map ! MessagePackage(Application.APP_NAME, messages.toArray)
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
    val tweetFilter = new FilterQuery
    tweetFilter.locations(locations(0), locations(1))
    tweetFilter // TODO This line can be removed as tweetFilter returns itself
  }

  /**
    * If a Tweet is geotagged, returns Location with these coordinates.
    * Otherwise, returns Location of the approximate place of the tweet.
    *
    * @param tweet Tweet status from Twitter API
    * @return Location
    */
  private def extractGeolocation(tweet: Status): Location = {
    if (tweet.getGeoLocation != null) {
      new Location(tweet.getGeoLocation.getLatitude, tweet.getGeoLocation.getLongitude)
      //TODO Case classes can be used without constructor. Location(x, y) is enough. This applies to all case classes.
    } else {
      getApproximatePlaceGeolocation(tweet)
    }
  }

  /**
    * Creates Location basing on the average values of latitudes and longitudes of Tweet's Place Bounding Box coordinates.
    *
    * @return Location
    */
  private def getApproximatePlaceGeolocation(tweet: Status): Location = {
    val boundingBoxCoordinates = tweet.getPlace.getBoundingBoxCoordinates
    val latitudes = boundingBoxCoordinates.flatMap(a => a.map(_.getLatitude))
    val longitudes = boundingBoxCoordinates.flatMap(a => a.map(_.getLongitude))

    new Location((latitudes.min + latitudes.max) / 2, (longitudes.min + longitudes.max) / 2)
  }

}

object SparkActor {
  lazy val actor = Akka.system().actorOf(Props[SparkActor])

  def apply() = actor
}

case class SparkGatheringJob(locations: Array[Array[Double]])
