# -*- coding:utf-8 -*-
import os
import time
from beehive.beehivelogger import *

__author__ = 'kevin'


def execute_cmd(cmd, db_name, tb_name):
    print cmd
    exit_status = os.system(cmd)
    if exit_status != 0:
        log.error("fail to insert:%s.%s" % (db_name, tb_name))


def main():
    current_path = os.path.abspath('.')
    conf_file = "%s/../configure/paydayloan_tbs.sql" % current_path
    read_file = open(conf_file)
    for kv in read_file:
        a = kv.replace('\n', '').split('=')
        tb_name = a[0]
        hive_hi_name = "o_hi_%s" % tb_name
        hive_tb_name = "o_%s" % tb_name
        sql = "hive -e 'insert into %s select * from %s;'" % (hive_tb_name,hive_hi_name)
        execute_cmd(sql,"paydayloan",hive_tb_name)
    read_file.close()

if __name__ == '__main__':
    begin = time.time()
    class_name = os.path.basename(__file__)
    log = logging.getLogger('beehive.%s' % class_name)
    log.info("begin %s" % class_name)
    try:
        main()
    except Exception, e:
        log.error(e)
    log.info("end %s" % class_name)
    end = time.time()
    log.info('total cost:%s minutes' % (round((end - begin) / 60, 3)))