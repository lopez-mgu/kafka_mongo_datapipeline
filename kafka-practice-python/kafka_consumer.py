from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient
import matplotlib.pyplot as plt
import numpy as np
from config import config
from mongo_functions import createMongoDB
import json



def process_topic_pets(mongoCol, data):
    data = json.loads(data)
    mongoCol.insert_one(data)

def process_topic_games(mongoCol, data):
    data = json.loads(data)
    mongoCol.insert_one(data)

def process_topic_weather(mongoCol, data):
    data = json.loads(data)
    res = [{'time':time,'temp':temp,'hum':hum} for time, temp, hum in zip(*data.values())]
    for res_val in res:
        if not len(res_val) == 0:
            mongoCol.insert_one(res_val)

def set_consumer_configs():
    config['group.id'] = 'hello_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False

def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')


def main():
    

    set_consumer_configs()
    consumer = Consumer(config)
    consumer.subscribe(['pet-types', 'NHL-games', 'weather-forecast'], on_assign=assignment_callback)

    weatherCol, petsCol, gamesCol = createMongoDB()

    func_dict = {'pet-types': process_topic_pets,  'NHL-games': process_topic_games, 'weather-forecast': process_topic_weather}
    mongo_dict = {'pet-types': petsCol,  'NHL-games': gamesCol, 'weather-forecast': weatherCol}

    try:
        while True:
            msg = consumer.poll()
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Parse the received message
                data = msg.value().decode('utf-8')
                partition = msg.partition()
                print(f'Received: {data} from partition {partition}    ')
                func_dict[msg.topic()](mongo_dict[msg.topic()], data) 

    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        # Close the consumer gracefully
        consumer.close()  


    print('Sesion Terminated...')
        
if __name__ == '__main__':
    main()
