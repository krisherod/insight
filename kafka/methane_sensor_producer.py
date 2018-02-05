#!/usr/bin/env python
import threading, logging, time
from kafka import KafkaConsumer, KafkaProducer
import datetime
import random
import config
import kafka_connector


def kafka_connector(kafka_servers):
    """
    
    Connect to Kafka

    """

    # check that there is a list
    if isinstance(kafka_servers, list):

        if len(kafka_servers)>0:

            try:
                return KafkaProducer(bootstrap_servers=kafka_servers)
            except ValueError:
                print ('Could not connect to Kafka with server addresses provide')
        else:
           raise ValueError('No elements in kafka_servers list')
    else:
        raise ValueError('No list was supplied for kafka_servers')
 



def main():

    # multiplication_factor = 0

    ## working code
    # producer = KafkaProducer(
        # bootstrap_servers=['ec2-52-11-90-50.us-west-2.compute.amazonaws.com:9092', 'ec2-34-216-187-59.us-west-2.compute.amazonaws.com:9092', 'ec2-52-41-188-179.us-west-2.compute.amazonaws.com'])

    producer = kafka_connector(config.kafka_servers)


    counter = 0     # used for counting how many sensor values have passed
    num_sites = 6000       # speficy the number of sites being used
    resolution = 1

    lag, lag_time = [], 0       # variable to save the value that is to be lagged

    while 1:

        with open(config.methane_data_file_name) as reader:

            for i,line in enumerate(reader):

                counter += 1

                if i ==0: continue

                # adjust the time in the list before it is produced to kafka
                line = line.split('\t')
                line[2] = str(int(time.time()))
                line[0] = str(int(line[0]))
                line[1] = str(int(line[1]))
                line = '\t'.join(line)



                # generate out of order values

                # check there is a lag record stored
                if len(lag)>0:
                    # if the lag time is 0 then send the value
                    if lag_time == 0:
                        producer.send(config.methane_kafka_topic, lag.encode())
                        lag = []
                        
                    else:
                        lag_time -= 1
                    
                # generate lag value every 1 in a 100 times
                if random.randint(0,100) == 10:
                    lag = line
                    lag_time = random.randint(1,15)



                try:
                    producer.send(config.methane_kafka_topic, line.encode())\

                except Exception as inst:
                    raise ValueError(\
                        'The following error occured when attempting to send data to \
                        methane topic: %s' %str(inst))

                if counter >= num_sites*2:
                    print (counter)
                    counter = 0
                    time.sleep(resolution)
            
        
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
level=logging.INFO
        )


    main()

