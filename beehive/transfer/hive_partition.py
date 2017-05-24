import os
import time
import sys

from beehive.beehivelogger import *

__author__ = 'kevin'


def create_partition(db, tb, dat):
    hive_cmd = "hive -e 'alter table %s.%s add if not exists partition(dat='%s');'" % (db, tb, dat)
    print hive_cmd
    exit_val = os.system(hive_cmd)
    if exit_val != 0:
        log.error("fail to create partition:%s.%s.%s" % (db, tb, dat))
        log.error(hive_cmd)


def main(argv):
    # create hive table partitions
    dat = argv[0]
    dbs = ["default"]
    tbs = ['o_comment', 'o_activity']
    for db in dbs:
        for tb in tbs:
            create_partition(db, tb, dat)
    log.info("complete creating partitions")


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
    log.info('total cost:%s minutes' % (round((end - begin) / 60, 3)))
