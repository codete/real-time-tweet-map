package com.codete.tweetmap.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.event.LoggingReceive
import play.libs.Akka

/**
  * Keeps track of map users (subscribing and terminating), dispatches messages.
  */
class MapActor extends Actor with ActorLogging {
  var users = Set[ActorRef]()

  def receive = LoggingReceive {
    case m: MessagePackage => users foreach {
      _ ! m
    } //TODO `m` can be replaced with msg or something more meaningful as it was misleading for me
    case Subscribe => {
      users += sender
      context watch sender
    }
    case Terminated(user) => users -= user
  }
}

object MapActor {
  lazy val map = Akka.system().actorOf(Props[MapActor])

  def apply() = map
}

case class MessagePackage(uuid: String, messages: Array[Message])

case class Message(text: String, location: Location)

case class Location(lat: Double, long: Double)

object Subscribe