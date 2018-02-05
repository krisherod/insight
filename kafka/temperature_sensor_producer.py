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
    # producer = kafka_connector.KafkaProducer(bootstrap_servers=['ec2-52-11-90-50.us-west-2.compute.amazonaws.com:9092', 'ec2-34-216-187-59.us-west-2.compute.amazonaws.com:9092', 'ec2-52-41-188-179.us-west-2.compute.amazonaws.com'])

    producer = kafka_connector.kafka_connector(config.kafka_servers)


    counter = 0     # used for counting how many sensor values have passed
    num_sites = 6000       # speficy the number of sites being used
    resolution = 1



    while 1:

        with open(config.temperature_data_file_name) as reader:

            for i,line in enumerate(reader):

                # print (line)

                counter += 1

                if i ==0: continue

                
                line = line.split('\t')

                line[0] = str(int(line[0]))
                line[1] = str(int(line[1]))
                line[2] = str(int(time.time()))
                line = '\t'.join(line)


                try:
                    producer.send(config.temperature_kafka_topic, line.encode())
                    o
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

    from sys import argv

    main()

