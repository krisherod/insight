
import psycopg2
import sys
from kafka import KafkaConsumer
import config

 
def main():
	#Define our connection string
	conn_string = "host='%s' port='%s' dbname='%s' user='%s' password='%s'" %(config.db_host, config.port, config.name, config.user_name. config.password)
 
	# print the connection string we will use to connect
	print ("Connecting to database\n	->%s" % (conn_string))
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()

	if cursor:
		print ("Connected!\n")

	# subscribe to kafka consumer

	consumer = KafkaConsumer(bootstrap_servers='%s:9092'%config.kafka_host, auto_offset_reset='earliest', consumer_timeout_ms=1000)


	# columns

	while 1:

		consumer.subscribe(['%s'%config.kafka_topic])

		# only commit to database every 100 values
		for _ in range(100):

			for message in consumer:
				print (message.value.split(','))

				message_parsed = message.value.split(',')

				# the incoming message must be of length 9
				if len(message_parsed) == 9:

					# create a prepared sql statement
					sql_statement = 'INSERT INTO %s (station_id, group_id, latitude, longitude, concentration, warning_status, alert_status, device_status) \
				 	VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (station_id) DO \
				 	UPDATE SET station_id=%s, group_id=%s, latitude=%s, longitude=%s, concentration=%s, warning_status=%s, alert_status=%s, device_status=%s' %('station_status', message_parsed[0], message_parsed[1], message_parsed[2], message_parsed[4], message_parsed[5], message_parsed[6], message_parsed[7], message_parsed[8], message_parsed[0], message_parsed[1], message_parsed[2], message_parsed[4], message_parsed[5], message_parsed[6], message_parsed[7], message_parsed[8])
					
					# print (sql_statement)
					cursor.execute(sql_statement)

			conn.commit()


	
 
if __name__ == "__main__":
	main()