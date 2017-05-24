# -*- coding:utf8 -*-
import os
import time

import sys

from beehive.beehivelogger import *
from beehive.configure.properties import Properties
from beehive.transfer.utils.sqoop_utils import HiveUtil

__author__ = 'kevin'


def execute_sqoop(sqoop_cmd, db_name, tb_name):
    print sqoop_cmd
    exit_status = os.system(sqoop_cmd)
    if exit_status != 0:
        log.error("fail to import:%s.%s" % (db_name, tb_name))


def main(argv):
    # input
    dat = argv[0]
    # load config
    current_path = os.path.abspath('.')
    # modify
    conf_file = "%s/../configure/mysql.properties" % current_path
    prop = Properties()
    conf = prop.getProperties(conf_file)
    sep = HiveUtil.sep

    # modify 1.movie
    db_name = "movie"
    host = conf.get("%s.host" % db_name)
    port = conf.get("%s.port" % db_name)
    username = conf.get("%s.username" % db_name)
    password = conf.get("%s.password" % db_name)
    # 1.1 import movie
    tb_name = "movie"
    hive_tb_name = "o_%s" % tb_name
    columns = ""
    # modify
    cmd = HiveUtil.templete_sql_partition()
    sqoop_cmd = cmd % ({'host': host, 'port': port, 'username': username, 'password': password,
                        'db_name': db_name, 'tb_name': tb_name, 'sep': sep, 'columns': columns,
                        'dat': dat, 'hive_tb_name': hive_tb_name})
    execute_sqoop(sqoop_cmd, db_name, tb_name)

    # 1.2 import cinema
    tb_name = "cinema"
    hive_tb_name = "o_%s" % tb_name
    columns = ""
    sqoop_cmd = cmd % ({'host': host, 'port': port, 'username': username, 'password': password,
                        'db_name': db_name, 'tb_name': tb_name, 'sep': sep, 'columns': columns,
                        'dat': dat, 'hive_tb_name': hive_tb_name})
    execute_sqoop(sqoop_cmd, db_name, tb_name)


if __name__ == '__main__':
    begin = time.time()
    class_name = os.path.basename(__file__)
    log = logging.getLogger('beehive.%s' % class_name)
    if len(sys.argv) != 2:
        log.error("Usage: %s <dat>" % class_name)
        sys.exit(-1)
    log.info("begin %s" % class_name)
    log.info(sys.argv[1:])
    try:
        main(sys.argv[1:])
    except Exception, e:
        log.error(e)
    log.info("end %s" % class_name)
    end = time.time()
    log.info('total cost:%s minutes' % round((end - begin) / 60, 3))
