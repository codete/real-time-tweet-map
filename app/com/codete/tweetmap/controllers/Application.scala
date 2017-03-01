package com.codete.tweetmap.controllers

import com.codete.tweetmap.actors.{SparkActor, SparkGatheringJob, UserActor}
import com.codete.tweetmap.utils.AppProperties
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
object Application extends Controller {

  val USA_BOUNDING_BOX = Array(Array(-124.0, 26.0), Array(-65.0, 49.0))
  val WORLD_BOUNDING_BOX = Array(Array(-180.0, -90.0), Array(180.0, 90.0))

  private var counter = 0

  /**
    * Schedules SparkActor initialization and running.
    */
  Akka.system.scheduler.scheduleOnce(AppProperties.GATHERING_DELAY seconds, SparkActor.actor, SparkGatheringJob(WORLD_BOUNDING_BOX))

  /**
    * Index endpoint of the web app.
    *
    * @param isMarkerMap specifies whether a marker or heat map view should be rendered
    * @return view to be rendered
    */
  def index(isMarkerMap: Boolean) = Action { implicit request => {
    val uid = request.session.get(AppProperties.UID).getOrElse {
      counter += 1
      counter.toString
    }
    Ok(com.codete.tweetmap.views.html.index(uid, isMarkerMap)).withSession {
      Logger.debug("Session UID: " + uid)
      request.session + (AppProperties.UID -> uid)
    }
  }
  }

  /**
    * WebSocket connection endpoint.
    *
    * @return UID assigned to the user / Forbidden status
    */
  def ws = WebSocket.tryAcceptWithActor[JsValue, JsValue] { implicit request =>
    Future.successful(request.session.get(AppProperties.UID) match {
      case None => Left(Forbidden)
      case Some(uid) => Right(UserActor.props(uid))
    })
  }

}