# -*- coding:utf8 -*-
import pymysql

from beehive.utils.properties import Properties

__author__ = 'wangcx'


class MysqlClient:
    def __init__(self, dbname):
        # load config
        conf_file = "../../../userprofile/config-mysql.properties"
        prop = Properties()
        conf = prop.getProperties(conf_file)

        host = conf.get("logcenter.host")
        port = int(conf.get("logcenter.port"))
        username = conf.get("logcenter.username")
        password = conf.get("logcenter.password")

        try:
            self.conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=dbname)
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
