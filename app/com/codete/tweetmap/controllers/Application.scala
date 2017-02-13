package com.codete.tweetmap.controllers

import com.codete.tweetmap.actors.{SparkActor, SparkGatheringJob, UserActor}
import org.apache.spark.streaming.Milliseconds
import play.api.Logger
import play.api.Play.current
import play.api.libs.concurrent.Akka
import play.api.libs.json.JsValue
import play.api.mvc.{Action, Controller, WebSocket}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
  * Main Controller and configuration properties holder.
  */
//TODO we have a lot of credentials. Maybe they can be moved to separate properties file ? Even in build.sbt will be good.
// http://stackoverflow.com/questions/25665848/how-to-load-setting-values-from-a-java-properties-file
object Application extends Controller {

  System.setProperty("twitter4j.oauth.consumerKey", "<TWITTER-CONSUMER-KEY>")
  System.setProperty("twitter4j.oauth.consumerSecret", "<TWITTER-CONSUMER-SECRET>")
  System.setProperty("twitter4j.oauth.accessToken", "<TWITTER-ACCESS-TOKEN>")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "<TWITTER-ACCESS-TOKEN-SECRET>")

  val UID = "uid"
  val APP_NAME = "codete.com"
  val SPARK_URL = "local[*]" // TODO - Just a double check. Main purpose of this is simple research project and we will probably not gonna use external cluster ever ?
  val SPARK_BATCH_DURATION = Milliseconds(500)

  val USA_BOUNDING_BOX = Array(Array(-124.0, 26.0), Array(-65.0, 49.0))
  val WORLD_BOUNDING_BOX = Array(Array(-180.0, -90.0), Array(180.0, 90.0))
  // TODO Array of arrays ? In Scala we have pair and lot of good features to be shorter with that

  private var counter = 0

  /**
    * Schedules SparkActor initialization and running.
    */
  Akka.system.scheduler.scheduleOnce(3 seconds, SparkActor.actor, SparkGatheringJob(WORLD_BOUNDING_BOX))

  /**
    * Index endpoint of the web app.
    *
    * @param isMarkerMap specifies whether a marker or heat map view should be rendered
    * @return view to be rendered
    */
  def index(isMarkerMap: Boolean) = Action { implicit request => {
    val uid = request.session.get(UID).getOrElse {
      counter += 1
      counter.toString
    }
    Ok(com.codete.tweetmap.views.html.index(uid, isMarkerMap)).withSession {
      Logger.debug("Session UID: " + uid)
      request.session + (UID -> uid)
    }
  }
  }

  /**
    * WebSocket connection endpoint.
    *
    * @return UID assigned to the user / Forbidden status
    */
  def ws = WebSocket.tryAcceptWithActor[JsValue, JsValue] { implicit request =>
    Future.successful(request.session.get(UID) match {
      case None => Left(Forbidden)
      case Some(uid) => Right(UserActor.props(uid))
    })
  }

}