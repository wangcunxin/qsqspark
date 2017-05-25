import os

from beehive.beehivelogger import *
from beehive.utils.mysql_client import MysqlClient

__author__ = 'kevin'


# execute sql and save to db
class BaseService:
    def __init__(self):
        class_name = os.path.basename(__file__)
        log = logging.getLogger('beehive.%s' % class_name)

    # parquet
    def _write_parquet(self, df, output_path):
        try:
            # df.show()
            self.log.debug(output_path)
            df.coalesce(1).write.mode('append').parquet(output_path)
        except Exception, e:
            self.log.error(e)

    # file:file:///
    def _write_file(self, list, output_path):
        fo = None
        try:
            fo = open(output_path, 'w')
            for line in list:
                fo.write(line+'\n')

        except Exception, e:
            self.log.error(e)
        finally:
            try:
                if fo != None:
                    fo.close()
            except Exception, e:
                self.log.error(e)

    # file:hdfs://namenode:port/
    def _write_hdfs_file(self, list, output_path):

        pass

    # mysql
    def _write_mysql(self, list, delete_sql, insert_sql):
        if list != None and len(list) > 0:
            dao = MysqlClient("dbname")
            try:
                dao.delete(delete_sql)
                dao.insertMany(insert_sql, list)
            except Exception, e:
                self.log.error(e)

    # mysql:insert and update
    def _write_mysql_insert_update(self, insert_sql, insert_list, update_sql, update_list):

        if insert_list != None and len(insert_list) > 0 and update_list != None and len(update_list) > 0:
            dao = MysqlClient("dbname")
            try:
                dao.insertMany(insert_sql, insert_list)
                dao.insertMany(update_sql, update_list)
            except Exception, e:
                self.log.error(e)

