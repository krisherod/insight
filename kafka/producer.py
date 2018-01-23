#!/usr/bin/env python
import threading, logging, time
import multiprocessing
from kafka import KafkaConsumer, KafkaProducer
import datetime
        
        
def main():

    ## working code
    producer = KafkaProducer(bootstrap_servers='ec2-34-213-63-132.us-west-2.compute.amazonaws.com:9092')

    # get the last time
    time_last = datetime.datetime(2000,1,1,0,0,0,0)


    with open('site_data.txt') as reader:

    	for i,line in enumerate(reader):

            # the frst value is the header
    		if i==0: continue

            # get the datetime from the column
    		time = datetime.datetime.strptime("2000-01-01 "+line.split('\t')[2], "%Y-%m-%d %H:%M:%S")

    		producer.send('my-topic2', line.encode())

    		for message in consumer:
    			print ('\n', message)
    			print (message.value)


    		if tim != tim_last:
    			time.sleep(1)
    			tim_last= tim_last+datetime.timedelta(minutes=1)


        
        
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
level=logging.INFO
        )
    main()


