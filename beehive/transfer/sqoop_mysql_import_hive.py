# -*- coding:utf8 -*-
import os
import time
import sys

from beehive.beehivelogger import *
from beehive.transfer.utils.sqoop_utils import HiveUtil
from beehive.utils.properties import Properties

__author__ = 'kevin'


def execute_sqoop(sqoop_cmd, db_name, tb_name):
    print sqoop_cmd
    exit_status = os.system(sqoop_cmd)
    if exit_status != 0:
        log.error("fail to import:%s.%s" % (db_name, tb_name))


def create_partition(db, tb, dat):
    hive_cmd = "hive -e 'alter table %s.%s add if not exists partition(dt='%s');'" % (db, tb, dat)
    print hive_cmd
    exit_val = os.system(hive_cmd)
    if exit_val != 0:
        log.error("fail to create partition:%s.%s.%s" % (db, tb, dat))
        log.error(hive_cmd)


def main(argv):
    begin_date = argv[0]
    end_date = argv[1]
    # load config
    current_path = os.path.abspath('.')
    # modify
    conf_file = "%s/../configure/mysql.properties" % current_path
    prop = Properties()
    conf = prop.getProperties(conf_file)
    sep = HiveUtil.SEP
    dt = argv[2]

    # load db
    db_name = "riskdata"
    host = conf.get("%s.host" % db_name)
    port = conf.get("%s.port" % db_name)
    username = conf.get("%s.username" % db_name)
    password = conf.get("%s.password" % db_name)

    conf_file = "%s/../configure/%s_tbs.sql" % (current_path,db_name)
    read_file = open(conf_file)

    conf_file = "%s/../configure/%s_columns.sql" % (current_path,db_name)
    column_map = prop.getProperties(conf_file)

    for kv in read_file:
        if kv.find("#") >= 0:
            continue
        a = kv.replace('\n', '').split('=')
        tb_name = a[0]
        update_time = a[1]
        hb_name = "o_%s" % tb_name
        columns = column_map.get(tb_name)
        if '-'.__eq__(update_time):
            cmd = HiveUtil.templete_sql_hive()
        else:
            create_partition(db_name, hb_name, dt)
            cmd = HiveUtil.templete_sql_hive_append()

        sqoop_cmd = cmd % ({'host': host, 'port': port, 'username': username, 'password': password,
                            'db_name': db_name, 'tb_name': tb_name, 'sep': sep, 'hb_name': hb_name,
                            'dt': dt, 'columns': columns,
                            'update_time': update_time, 'begin_date': begin_date, 'end_date': end_date})
        execute_sqoop(sqoop_cmd, db_name, tb_name)

    read_file.close()


if __name__ == '__main__':
    begin = time.time()
    class_name = os.path.basename(__file__)
    log = logging.getLogger('beehive.%s' % class_name)
    if len(sys.argv) != 4:
        log.error("Usage: %s <begin:2017-01-01> <end> <dt>" % class_name)
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
