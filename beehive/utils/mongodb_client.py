# -*- coding:utf8 -*-
import os

from pymongo import MongoClient

from beehive.utils.properties import Properties

__author__ = 'wangcx'


class MongodbClient:
    def __init__(self, db_name):
        # load config
        current_path = os.path.abspath('.')
        conf_file = "%s/../configure/mongodb.properties" % current_path
        prop = Properties()
        conf = prop.getProperties(conf_file)

        host = conf.get("%s.host" % db_name)
        port = conf.get("%s.port" % db_name)
        username = conf.get("%s.username" % db_name)
        password = conf.get("%s.password" % db_name)

        self.mongoClient = MongoClient(host, port)
        self.mongoDatabase = self.mongoClient.get_database(db_name)
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
