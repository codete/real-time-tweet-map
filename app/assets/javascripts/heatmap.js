var webSocket;
var googleMap;
var heatMapLayer;
var points;
var heatmapPointsCountLimit = 5000;

function initMap() {
    points = new google.maps.MVCArray();

    googleMap = new google.maps.Map(document.getElementById('map'), {
        zoom: 12,
        center: {lat: 50.0725115, lng: 19.9558725},
        mapTypeId: google.maps.MapTypeId.MAP
    });

    heatMapLayer = new google.maps.visualization.HeatmapLayer({
        data: points,
        map: googleMap,
        radius: 20
    });
}

$(document).ready(function () {
    webSocket = new WebSocket($("body").data("ws-url"));
    webSocket.onmessage = onMessage;
});

function onMessage(event) {
    var data = JSON.parse(event.data);
    addLocations(data.messages);
}

function addLocations(messages) {
    _(messages).each(function (message) {
        addLocation(message.location);
    });
    removeOldestLocationsIfLimitReached();
}

function addLocation(location) {
    points.push(new google.maps.LatLng(location.lat, location.long));
}

function removeOldestLocationsIfLimitReached() {
    while (points.length > heatmapPointsCountLimit) {
        points.removeAt(0);
    }
}