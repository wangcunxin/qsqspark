# -*- coding:utf8 -*-

__author__ = 'kevin'


class HiveUtil:

    SEP = "\001"
    CF = "cf"
    @staticmethod
    def templete_sql_hive():
        cmd = '''
        sqoop import \
        --connect "jdbc:mysql://%(host)s:%(port)s/%(db_name)s?tinyInt1isBit=false" \
        --username %(username)s \
        --password %(password)s \
        --table "%(tb_name)s" \
        --columns "%(columns)s" \
        --fields-terminated-by %(sep)s \
        --hive-drop-import-delims \
        --target-dir /user/sqoop2/%(db_name)s/%(hb_name)s  \
        --append \
        -m 1;
        '''
        return cmd

    @staticmethod
    def templete_sql_hive_append():
        cmd = '''
        sqoop import \
        --connect "jdbc:mysql://%(host)s:%(port)s/%(db_name)s?tinyInt1isBit=false" \
        --username %(username)s \
        --password %(password)s \
        --table "%(tb_name)s" \
        --columns "%(columns)s" \
        --where "%(update_time)s>='%(begin_date)s' and %(update_time)s<'%(end_date)s'" \
        --fields-terminated-by %(sep)s \
        --hive-drop-import-delims \
        --target-dir /user/sqoop2/%(db_name)s/%(hb_name)s/dt='%(dt)s'  \
        --append \
        -m 1;
        '''
        return cmd

    @staticmethod
    def templete_sql_hbase_append():
        cmd = '''
        sqoop import \
        --connect "jdbc:mysql://%(host)s:%(port)s/%(db_name)s?tinyInt1isBit=false" \
        --username %(username)s \
        --password %(password)s \
        --table %(tb_name)s \
        --where "%(update_time)s>='%(begin_date)s' and %(update_time)s<'%(end_date)s'" \
        --hbase-table %(db_name)s:%(hb_name)s \
        --hbase-create-table \
        --hbase-row-key %(key)s \
        --column-family %(cf)s \
        --m 1;
        '''
        return cmd
    @staticmethod
    def templete_sql_hbase():
        cmd = '''
        sqoop import \
        --connect "jdbc:mysql://%(host)s:%(port)s/%(db_name)s?tinyInt1isBit=false" \
        --username %(username)s \
        --password %(password)s \
        --table %(tb_name)s \
        --hbase-table %(db_name)s:%(hb_name)s \
        --hbase-create-table \
        --hbase-row-key %(key)s \
        --column-family %(cf)s \
        --m 1;
        '''
        return cmd
    pass
