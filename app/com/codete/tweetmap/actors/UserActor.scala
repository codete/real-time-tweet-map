package com.codete.tweetmap.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingReceive
import play.api.libs.json.{JsObject, Json}

/**
  * Handles communication with enduser.
  *
  * @param uid user UID
  * @param out output stream reference (WebSocket output)
  */
class UserActor(uid: String, out: ActorRef) extends Actor with ActorLogging {

  /**
    * Subscribes to the MapActor for obtaining tweet packages.
    */
  override def preStart() = {
    MapActor() ! Subscribe
  }

  /**
    * Converts MessagePackage into JSON object and sends it to the enduser through the WebSocket connection.
    */
  def receive = LoggingReceive {
    case MessagePackage(muid, messages) if sender == MapActor() => {
      val json: JsObject = Json.obj("uid" -> muid, "messages" -> messages.map(m => Json.obj("text" -> m.text, "location" -> Json.obj("lat" -> m.location.lat, "long" -> m.location.long))))
      // TODO above one could be moved to dedicated mapping method.
      out ! json
    }
  }

}

object UserActor {
  def props(uid: String)(out: ActorRef) = Props(new UserActor(uid, out))
}
