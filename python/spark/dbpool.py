# -*- coding:utf-8 -*-
from DBUtils.PooledDB import PooledDB

args = {"host":"192.168.8.108:3306", "user":"logcenter", "passwd":"logcenter123", "db":"logcenter" }

pool = PooledDB(args)


def getConnection():
    return pool.connection()