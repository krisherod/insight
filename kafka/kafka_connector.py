from kafka import KafkaConsumer, KafkaProducer

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
 

