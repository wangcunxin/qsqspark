# -*- coding:utf8 -*-

__author__ = 'kevin'


class HiveUtil:

    sep = "\001"

    @staticmethod
    def templete_sql_partition():
        cmd = '''
        sqoop import \
        --connect "jdbc:mysql://%(host)s:%(port)s/%(db_name)s" \
        --username %(username)s \
        --password %(password)s \
        --table "%(tb_name)s" \
        --columns "%(columns)s" \
        --where "to_char(ADDTIME,'yyyyMMdd')='%(dat)s'" \
        --fields-terminated-by %(sep)s \
        --hive-drop-import-delims \
        --target-dir /user/sqoop2/%(hive_tb_name)s/dt='%(dat)s'  \
        --append \
        -m 1;
        '''
        return cmd

    @staticmethod
    def templete_sql():
        cmd = '''
        sqoop import \
        --connect "jdbc:mysql://%(host)s:%(port)s/%(db_name)s" \
        --username %(username)s \
        --password %(password)s \
        --table "%(tb_name)s" \
        --columns "%(columns)s" \
        --fields-terminated-by %(sep)s \
        --hive-drop-import-delims \
        --target-dir /user/sqoop2/%(hive_tb_name)s  \
        --append \
        -m 1;
        '''
        return cmd

    pass
