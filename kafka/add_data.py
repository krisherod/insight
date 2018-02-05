import psycopg2
import sys
from kafka import KafkaConsumer
from datetime import datetime
import config



def connect_to_db():


    """
    
    Connect to the database

    """

    #Define our connection string
    conn_string = "host='%s' port='%s' dbname='%s' user='%s' password='%s'"\
    %(config.db_host, config.db_port, config.db_name, config.db_user_name, config.db_password)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    return conn.cursor()




def insert_data(cursor, message_parsed):

    """
    insert data from kafka into real time table and batch table


    """

    # add data to the current table
    # station_id, group_id, averageValue, latitude, longitude, warning_status, alert_status, device_status
    sql_statement = "INSERT INTO %s \
    (station_id, group_id, latitude, longitude, concentration, warning_status, alert_status, device_status, update_timestamp) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, current_timestamp) \
    ON CONFLICT (station_id) DO \
    UPDATE SET \
    station_id=%s, group_id=%s, latitude=%s, longitude=%s, concentration=%s, warning_status=%s, \
    alert_status=%s, device_status=%s, update_timestamp=current_timestamp" \
    %('station_status', message_parsed[0], message_parsed[1], message_parsed[3], message_parsed[4], \
        message_parsed[2], message_parsed[5], message_parsed[6], message_parsed[7], \
        message_parsed[0], message_parsed[1], message_parsed[3], message_parsed[4], \
        message_parsed[2], message_parsed[5], message_parsed[6], message_parsed[7])

    cursor.execute(sql_statement)




 
def main():
    
    """
    
    Get data from Kafka topic and put it into a postgres table

    """

    # connect to the database
    cursor = None
    while not cursor:
        try:cursor = connect_to_db()
        except:cursor=None


    # input schema: station_id, group_id, concentration, timestamp, lat, long, warning status, alert_status, broken_status
    consumer = KafkaConsumer(bootstrap_servers=config.kafka_servers, \
                            auto_offset_reset='latest', \
                            consumer_timeout_ms=1000)


    # continue checking
    while 1:

        consumer.subscribe([config.kafka_consumer_topic])

        # only commit every 100 rows

        for _ in range(100):

            for message in consumer:

                message_parsed = message.value.split(',')
                if len(message_parsed) == 8:
                    insert_data(cursor, message_parsed)

            conn.commit()



 
if __name__ == "__main__":
        main()


