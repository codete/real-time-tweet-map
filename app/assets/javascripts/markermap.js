var webSocket;
var googleMap;
var tweets = [];
var markerCountLimit = 300;
var currentlyOpened = {
    marker: null,
    window: null
};

function initMap() {
    googleMap = new google.maps.Map(document.getElementById('map'), {
        zoom: 12,
        center: {lat: 50.0725115, lng: 19.9558725},
        mapTypeId: google.maps.MapTypeId.MAP
    });
}

$(document).ready(function () {
    webSocket = new WebSocket($("body").data("ws-url"));
    webSocket.onmessage = onMessage;
});

function onMessage(event) {
    var data = JSON.parse(event.data);
    displayNewTweets(data.messages);
}

function displayNewTweets(messages) {
    _(messages).each(function (message) {
        addNewMarker(message);
    });
    removeOldestMarkersIfLimitReached();
}

function addNewMarker(message) {
    var location = message.location;
    var infoWindow = createInfoWindow(message);
    var marker = createMapMarker(location);

    marker.addListener('click', onMarkerClick.bind(window, marker, infoWindow));
    tweets.push(marker);
}

function removeOldestMarkersIfLimitReached() {
    while (tweets.length > markerCountLimit) {
        var indexOfOldestNotOpenedMarker = tweets[0] === currentlyOpened.marker ? 1 : 0;
        tweets[indexOfOldestNotOpenedMarker].setMap(null);
        tweets.splice(indexOfOldestNotOpenedMarker, 1);
    }
}

function createInfoWindow(message) {
    return new google.maps.InfoWindow({
        content: message.text
    });
}

function createMapMarker(location) {
    return new google.maps.Marker({
        position: new google.maps.LatLng(location.lat, location.long),
        map: googleMap,
        visible: true
    });
}

function onMarkerClick(marker, infoWindow) {
    currentlyOpened.window && currentlyOpened.window.close();
    infoWindow.open(googleMap, marker);
    currentlyOpened = {
        marker: marker,
        window: infoWindow
    };
}
