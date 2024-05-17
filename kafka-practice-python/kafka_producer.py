import json
import logging
import time
import requests
from confluent_kafka import Producer
from config import config

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def GetAPIRequest(url):
    data = requests.get(url).json()
    return data

def GetWeatherData(LAT, LON):
    LAT = 28.6433753
    LON = -106.0587908
    Weather_API = f'https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&hourly=temperature_2m,relative_humidity_2m'
    weatherData = GetAPIRequest(Weather_API)
    forecastData = {'time':weatherData['hourly']['time'], 'temperature_2m':weatherData['hourly']['temperature_2m'], 'relative_humidity_2m':weatherData['hourly']['relative_humidity_2m'],}
    return forecastData

def GetPetsData():
    Pets_API = 'https://dogapi.dog/api/v2/breeds'
    PetsData = GetAPIRequest(Pets_API)['data']
    BreedsData = []
    for breed in PetsData:
        BreedsData.append({'breed_name':breed['attributes']['name'], 'life_min':breed['attributes']['life']['min'], 'life_max':breed['attributes']['life']['max']})
    return BreedsData

def GetNHLData(gameDate):
    NHL_API = f'https://api.nhle.com/stats/rest/en/team/summary?isAggregate=false&isGame=true&sort=%5B%7B%22property%22:%22points%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22wins%22,%22direction%22:%22DESC%22%7D,%7B%22property%22:%22teamId%22,%22direction%22:%22ASC%22%7D%5D&start=0&limit=50&cayenneExp=gameDate%3C=%22{gameDate}%2023%3A59%3A59%22%20and%20gameDate%3E=%22{gameDate}%22%20and%20gameTypeId=2'
    NHLData = GetAPIRequest(NHL_API)['data']
    gamesData = []
    for game in NHLData:
        gamesData.append({'gameDate':game['gameDate'], 'gameId':game['gameId'], 'goalsFor':game['goalsFor'], 'goalsAgainst':game['goalsAgainst']})
    return gamesData

def initkafkaBrokers():
    producer = Producer(config)
    print('Kafka Broker-1 has been initiated...')
    return producer


def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        

def main():
    LAT = 28.6433753
    LON = -106.0587908
    gameDate = '2024-04-18'

    broker_1 = initkafkaBrokers()

    weatherData =  GetWeatherData(LAT, LON)
    petsData = GetPetsData()
    gamesData = GetNHLData(gameDate)

    #send message to broker 1
    m1=json.dumps(weatherData)
    broker_1.poll(1)
    broker_1.produce('weather-forecast', m1.encode('utf-8'),callback=receipt)
    broker_1.flush()
    time.sleep(1)

    #send message to broker 2
    for pet in petsData:
        m2=json.dumps(pet)
        broker_1.poll(1)
        broker_1.produce('pet-types', m2.encode('utf-8'),callback=receipt)
        broker_1.flush()
        time.sleep(1)

    #send message to broker 2
    for game in gamesData:
        m3=json.dumps(game)
        broker_1.poll(1)
        broker_1.produce('NHL-games', m3.encode('utf-8'),callback=receipt)
        broker_1.flush()
        time.sleep(1)


if __name__ == '__main__':
    main()
    print('Session Terminated...')




 