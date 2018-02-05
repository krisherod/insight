#!/usr/bin/env python
import threading, logging, time
from kafka import KafkaConsumer, KafkaProducer
import datetime
import random
import config



def kafka_connector_producer(kafka_servers):
    """
    
    Connect to Kafka producer

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
 






def kafka_connector_consumer(kafka_servers):
    """
    
    Connect to Kafka consumer

    """

    print (kafka_servers)

    # check that there is a list
    if isinstance(kafka_servers, list):

        if len(kafka_servers)>0:

            try:
                return KafkaConsumer(bootstrap_servers=kafka_servers)
            except ValueError:
                print ('Could not connect to Kafka with server addresses provide')
        else:
           raise ValueError('No elements in kafka_servers list')
    else:
        raise ValueError('No list was supplied for kafka_servers')



def main():

    producer = kafka_connector_producer(config.kafka_servers)
    consumer = kafka_connector_consumer(config.kafka_servers)

    num_sites, resolution, results, counter = 4, 0.1, [], 0

    with open(config.methane_data_file_name_test) as methane_reader, open(config.temperature_data_file_name_test) as temperature_reader:

        for i, (methane_line, temperature_line) in enumerate(zip(methane_reader, temperature_reader)):
            counter += 1

            if i==0: continue

            # print (methane_line)
            # print (temperature_line)


            # adjust the time in the list before it is produced to kafka
            methane_line = methane_line.split('\t')
            methane_line[2] = str(int(time.time()))
            methane_line[0] = str(int(methane_line[0]))
            methane_line[1] = str(int(methane_line[1]))
            methane_line = '\t'.join(methane_line)

            temperature_line = temperature_line.split('\t')
            temperature_line[2] = str(int(time.time()))
            temperature_line[0] = str(int(temperature_line[0]))
            temperature_line[1] = str(int(temperature_line[1]))
            temperature_line = '\t'.join(temperature_line)


            try:
                # print (config.methane_kafka_topic)
                print (temperature_line)
                print (methane_line)

                producer.send(config.temperature_kafka_topic, temperature_line.encode())
                producer.send(config.methane_kafka_topic, methane_line.encode())

            except Exception as inst:
                raise ValueError(\
                    'The following error occured when attempting to send data to \
                    methane topic: %s' %str(inst))

            if counter >= num_sites*2:
                counter = 0
                time.sleep(resolution)

        print (i)


    try:
        print (config.kafka_consumer_topic)
        consumer.subscribe([config.kafka_consumer_topic])
        
        for message in consumer:
            # print (message)
            message_parsed = message.split(',')
            if len(message_parsed) == 8:
                results.append(message_parsed)

    except Exception as inst:
        raise ValueError(\
          'The following error occured when attempting to send data to \
            methane topic: %s' %str(inst))



# def main():

# 	"""
# 	main function for integration testing
# 	"""

# 	num_sites = 4
#     resolution = 1
#     results = []


#     with open(config.methane_data_file_name) as reader:
#         for i,line in enumerate(reader):
#             counter += 1

#             if i ==0: continue

#             try:
#                 producer.send(config.methane_kafka_topic, line.encode())\

#             except Exception as inst:
#                 raise ValueError(\
#                     'The following error occured when attempting to send data to \
#                     methane topic: %s' %str(inst))

#             try:
#             	consumer.subscribe([config.kafka_consumer_topic])

#                 for message in consumer:

#                     message_parsed = message.value.split(',')
#                     if len(message_parsed) == 8:
#                         results.append(message_parsed)

#             except Exception as inst:
#                 raise ValueError(\
#             		'The following error occured when attempting to send data to \
#                     methane topic: %s' %str(inst))


#             if counter >= num_sites*2:
#                 counter = 0
#                 time.sleep(resolution)




#     for result in results:
#         print (result)



	# return 0


if __name__=="__main__":
    main()
