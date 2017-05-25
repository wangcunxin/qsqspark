# -*- coding:utf8 -*-
import os
import pymysql

from beehive.utils.properties import Properties

__author__ = 'wangcx'


class MysqlClient:
    def __init__(self, db_name):
        # load config
        current_path = os.path.abspath('.')
        conf_file = "%s/../configure/mysql.properties" % current_path
        prop = Properties()
        conf = prop.getProperties(conf_file)

        host = conf.get("%s.host" % db_name)
        port = conf.get("%s.port" % db_name)
        username = conf.get("%s.username" % db_name)
        password = conf.get("%s.password" % db_name)

        try:
            self.conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=db_name)
            self.cursor = self.conn.cursor()
        except Exception, e:
            print e

    def __del__(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception, e:
            print e

    def query(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        return list(rows)

    def find_one(self, sql=None):
        self.cursor.execute(sql)
        return self.cursor.fetchone()

    def execute(self, executeSql=None):
        self.cursor.execute(executeSql)
        self.conn.commit()

    def insert_many(self, insertStr=None, list=None):
        self.cursor.executemany(insertStr, list)
        self.conn.commit()
