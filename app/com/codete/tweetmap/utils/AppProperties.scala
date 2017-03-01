package com.codete.tweetmap.utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.Milliseconds

object AppProperties {

  private val configuration = ConfigFactory.load()

  val UID = "uid"
  val SPARK_LOCAL_URL = "local[*]"
  val SPARK_BATCH_DURATION = Milliseconds(configuration.getInt("spark.batch-duration"))
  val GATHERING_DELAY = configuration.getInt("spark.gathering-delay-in-seconds")
  val APP_NAME = configuration.getString("application.name")

  val CONSUMER_KEY = configuration.getString("twitter4j.oauth.consumerKey")
  val CONSUMER_SECRET = configuration.getString("twitter4j.oauth.consumerSecret")
  val ACCESS_TOKEN = configuration.getString("twitter4j.oauth.accessToken")
  val ACCESS_TOKEN_SECRET = configuration.getString("twitter4j.oauth.accessTokenSecret")

}
