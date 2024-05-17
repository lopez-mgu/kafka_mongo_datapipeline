from pymongo import MongoClient
import requests
import matplotlib.pyplot as plt
import numpy as np


def CreateMongoDB(db_name):
    #Connecting to DB
    #db_client = MongoClient('localhost', 27017)

    db_client = MongoClient(host=['localhost:27017'],
                            username = "root",
                            password = "rootpassword",
                            authSource = "admin",
                            authMechanism = "SCRAM-SHA-256")
    #Create DB
    data = db_client[db_name]
    return data

def CreateMongoCollection(db_object, collection_name):
    #Create a collection
    storage = db_object[collection_name]
    return storage

def FindManyMongo(mongo_collection, query={}, key={}):
    data_list = []
    retrive_data = mongo_collection.find(query,key) #.find({'DEPARTMENT.DNAME':'SALES'},{ENAME:1,JOB:1,_id:0})
    for data in retrive_data:
        data_list.append(data)
    return data_list

def createMongoDB():
    MongoDB = CreateMongoDB('mongodb')
    weatherCol = CreateMongoCollection(MongoDB, 'weather')
    petsCol = CreateMongoCollection(MongoDB, 'pets')
    gamesCol = CreateMongoCollection(MongoDB, 'games')
    return weatherCol, petsCol, gamesCol
