package com.codete.tweetmap.utils

import com.codete.tweetmap.actors.Location
import twitter4j.Status

object GeolocationEstimator {

  /**
    * If a Tweet is geotagged, returns Location with these coordinates.
    * Otherwise, returns Location of the approximate place of the tweet.
    *
    * @param tweet Tweet status from Twitter API
    * @return Location
    */
  def estimateTweetGeolocation(tweet: Status): Location = {
    if (tweet.getGeoLocation != null) {
      Location(tweet.getGeoLocation.getLatitude, tweet.getGeoLocation.getLongitude)
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

    Location((latitudes.min + latitudes.max) / 2, (longitudes.min + longitudes.max) / 2)
  }

}
