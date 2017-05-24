# -*- coding:utf8 -*-
from pymongo import MongoClient

__author__ = 'wangcx'


class MongodbClient:
    def __init__(self, host, port, dbname, username, password):
        self.mongoClient = MongoClient(host, port)
        self.mongoDatabase = self.mongoClient.get_database(dbname)
        self.mongoDatabase.authenticate(username, password)

    def __del__(self):
        self.mongoClient.close()

    def setCollection(self, colName):
        self.mongoCollection = self.mongoDatabase.get_collection(colName)

    def findAll(self):
        return self.mongoCollection.find()

    def findWithQuery(self, doc=None, skip=None, limit=None):
        if skip is not None and limit is not None:
            return self.mongoCollection.find(doc).skip(0).limit(10)
        else:
            return self.mongoCollection.find(doc)

    def insertOne(self, doc=None):
        self.mongoCollection.insert(doc)

    def insertMany(self, docs):
        self.mongoCollection.insert_many(docs)
