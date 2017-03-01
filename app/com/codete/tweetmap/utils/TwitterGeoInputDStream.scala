package com.codete.tweetmap.utils

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._
import twitter4j.auth.{Authorization, OAuthAuthorization}
import twitter4j.conf.ConfigurationBuilder

/**
  * Implementation of ReceiverInputDStream based on implementation of
  * org.apache.spark.streaming.twitter.TwitterInputDStream
  * extended to allow both: keyword and geolocation filtering of tweets.
  *
  * @param ssc_   StreamingContext object
  * @param filter FilterQuery object for tweet filtering
  */
class TwitterGeoInputDStream(
                              ssc_ : StreamingContext,
                              filter: Option[FilterQuery]
                            ) extends ReceiverInputDStream[Status](ssc_) {

  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(
      new ConfigurationBuilder()
        .setOAuthConsumerKey(AppProperties.CONSUMER_KEY)
        .setOAuthConsumerSecret(AppProperties.CONSUMER_SECRET)
        .setOAuthAccessToken(AppProperties.ACCESS_TOKEN)
        .setOAuthAccessTokenSecret(AppProperties.ACCESS_TOKEN_SECRET)
        .build()
    )
  }

  private val authorization = createOAuthAuthorization()

  override def getReceiver(): Receiver[Status] = {
    new TwitterGeoReceiver(authorization, filter, StorageLevel.MEMORY_AND_DISK_SER_2)
  }

}

class TwitterGeoReceiver(
                          twitterAuth: Authorization,
                          filter: Option[FilterQuery],
                          storageLevel: StorageLevel
                        ) extends Receiver[Status](storageLevel) with Logging {

  @volatile private var twitterStream: TwitterStream = _
  @volatile private var stopped = false

  def onStart() {
    try {
      val newTwitterStream = new TwitterStreamFactory().getInstance(twitterAuth)
      newTwitterStream.addListener(new StatusListener {
        def onStatus(status: Status): Unit = {
          store(status)
        }

        // Unimplemented
        def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

        def onTrackLimitationNotice(i: Int) {}

        def onScrubGeo(l: Long, l1: Long) {}

        def onStallWarning(stallWarning: StallWarning) {}

        def onException(e: Exception) {
          if (!stopped) {
            restart("Error receiving tweets", e)
          }
        }
      })

      if (filter.nonEmpty) {
        newTwitterStream.filter(filter.get)
      } else {
        newTwitterStream.sample()
      }
      setTwitterStream(newTwitterStream)
      logInfo("Twitter receiver started")
      stopped = false
    } catch {
      case e: Exception => restart("Error starting Twitter stream", e)
    }
  }

  def onStop() {
    stopped = true
    setTwitterStream(null)
    logInfo("Twitter receiver stopped")
  }

  private def setTwitterStream(newTwitterStream: TwitterStream) = synchronized {
    if (twitterStream != null) {
      twitterStream.shutdown()
    }
    twitterStream = newTwitterStream
  }
}

