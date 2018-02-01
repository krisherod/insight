# coding: utf-8

from flask import Flask, render_template
from flask_googlemaps import GoogleMaps
from flask_googlemaps import Map, icons
import webbrowser
import psycopg2
from datetime import datetime
import json

app = Flask(__name__, template_folder="templates")

# GoogleMaps(app, key="AIzaSyDh7SGU7_tQYMe-Sbik39Rwosclm6TAZ4A")



# put this into another file
def getData():

        try:
            conn_string = "host='%s' port='%s' dbname='%s' user='%s' password='%s'"%(config.db_host, config.db_port, config.db_name, config.db_user_name, config.db_password)
            conn = psycopg2.connect(conn_string)

            cursor = conn.cursor()

            if cursor:
                    print ("connected to postgresql")

            sql_statement = "SELECT * FROM station_status WHERE current_timestamp>'"+str(datetime.now()-timedelta(seconds = 10))+"';"

            cursor.execute(sql_statement)

            station_data = []


            for station in cursor.fetchall():
                    station_data.append ({
                            'group_id': station[1],
                            'timestamp': str(station[2]),
                            'concentration': station[7],
                            'latitude': station[3],
                            'longitude': station[4],
                            'warning_status': station[5],
                            'alert_status': station[6],
                            'device_status': station[8]
                    })




            return station_data

        except:
            return [{'group_id': 100, 'timestamp': str(datetime.now()), 'concentration': 123, 'latitude': 43.0, 'longitude': -122.7, 'alert_status': 1, 'device_status': 0}]





@app.route('/getData')
def update():
    return json.dumps(getData())




@app.route('/')
def mymap():

    locations = [{'lat': 37.3,'lng': -122.3,'icon': 'http://maps.google.com/mapfiles/kml/pal3/icon59.png','infobox': '<b>asdfasdf</b>'}]
    sndmap = Map(
        identifier="sndmap",
        style="height:900px;width:900px",
        lat=locations[0]['lat'],
        lng=locations[0]['lng'],
        markers=[{'icon': loc['icon'], 'lat':loc['lat'], 'lng': loc['lng']} for loc in locations]
    )
    
    return render_template('map.html')




if __name__=="__main__":
	app.run()


