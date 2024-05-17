import matplotlib.pyplot as plt
from pymongo import MongoClient
from kafka_consumer import createMongoDB
from mongo_functions import FindManyMongo


def plotData(dates, temp, hum):
    # plot lines 
    plt.plot(dates, temp, label = "Average Temp (C)") 
    plt.plot(dates, hum, label = "Humidity")
    plt.legend() 
    plt.show()

def CreateListFromMongoQuery(MongoCol, ItemKey):
    MongoQuery = FindManyMongo(MongoCol, query={}, key={ItemKey:1,'_id':0})
    MongoList = []
    for item in MongoQuery:
        if not len(item) == 0:
            MongoList.append(item[ItemKey])
    return MongoList



if __name__ == '__main__':
    weatherCol, petsCol, gamesCol = createMongoDB()

    gameDates = CreateListFromMongoQuery(gamesCol, 'gameDate')
    print(gameDates)

    breed_name = CreateListFromMongoQuery(petsCol, 'breed_name')
    print(breed_name)

    breed_name

    dates = CreateListFromMongoQuery(weatherCol, 'time')
    temp = CreateListFromMongoQuery(weatherCol, 'temp')
    hum = CreateListFromMongoQuery(weatherCol, 'hum')

    plotData(dates, temp, hum)