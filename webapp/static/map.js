
var map;
var gmarkers=[];

function initMap() {
    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 10,
        center: {lat: 34.8, lng: -118.2}
    });
// console.log(map);
// gmarkers=[];
updateMap();

}

function updateMap() {

    function loadJSON(callback) {
        var xobj = new XMLHttpRequest();
        xobj.overrideMimeType("application/json");
        xobj.open('GET', '/update', true);
        xobj.onreadystatechange = function () {
            if (xobj.readyState == 4 && xobj.status == "200") {
                callback(xobj.responseText);
            }
        };
        xobj.send(null);
    }

    loadJSON(function(response) {

        console.log(gmarkers);
        if (gmarkers.length>0) {
            for (var i=0; i<gmarkers.length; i++) {
                gmarkers[i].setMap(null);
            }
        }

        // console.log(response);
        stations = JSON.parse(response);
        // console.log(beaches);
        // console.log(map);

        var markers = stations.map(function(res, i){
            
        var contentString = "<h1>Sensor: " + res.group_id + "</h1><br />";
            contentString += "Concentration " + res.concentration + " ppm<br />";
            contentString += "Position: " + res.latitude.toString() + ", ";
            contentString += res.longitude.toString();


        var infowindow = new google.maps.InfoWindow({
            content: contentString
        });


        if (res.alert_status ==1){
            var marker = new google.maps.Marker({
                    position: {'lat': res.latitude, 'lng': res.longitude},
                    map: map,
                    name: res.group_id,
                    icon: 'http://maps.google.com/mapfiles/kml/pal3/icon59.png'
                });
            }
            if (res.device_status==1){
                var marker = new google.maps.Marker({
                    position: {'lat': res.latitude, 'lng': res.longitude},
                    map: map,
                    name: res.group_id,
                    icon: 'http://maps.google.com/mapfiles/kml/pal3/icon43.png'
                });
            }

            gmarkers.push(marker);
            return marker;

        });

    });

}

window.setInterval(function(){
    updateMap();
}, 4000);
