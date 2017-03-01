package com.codete.tweetmap.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingReceive
import play.api.libs.json.{JsObject, Json}

/**
  * Handles communication with end user.
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
    * Converts MessagePackage into JSON object and sends it to the end user through the WebSocket connection.
    */
  def receive = LoggingReceive {
    case MessagePackage(muid, messages) if sender == MapActor() => {
      out ! mapMessageToJson(muid, messages)
    }
  }

  private def mapMessageToJson(muid: String, messages: Array[Message]): JsObject = {
    Json.obj("uid" -> muid, "messages" -> messages.map(m => Json.obj("text" -> m.text, "location" -> Json.obj("lat" -> m.location.lat, "long" -> m.location.long))))
  }

}

object UserActor {
  def props(uid: String)(out: ActorRef) = Props(new UserActor(uid, out))
}
