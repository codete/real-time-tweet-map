# Real-time Tweet Map

An application created at [Codete](http://codete.com/) for real-time tweets visualization on a map.

Read more about the application on [Codete Blog](http://codete.com/blog)

## Prerequisites
* Install Scala and Play Framework
* Checkout the repository
* Create a [Twitter App](https://apps.twitter.com/) and update Application.scala accordingly
* Enable Google Maps JavaScript API in your [Google Developer Console](https://console.developers.google.com/apis), create API key credential and paste it into index.scala.html

## Run
Run the application with command
```bash
activator run
```

Visit `http://localhost:9000` to display heatmap or `http:///localhost:9000/marker` to display marker map.