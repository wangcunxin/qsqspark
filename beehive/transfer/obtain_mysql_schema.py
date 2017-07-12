# -*- coding: utf-8 -*-

import pymysql
import re

conn = pymysql.connect(host='127.0.0.1', port=3307, user='kfq', passwd='as$h9LxgC1kH', db='paydayloan', charset='utf8')
cur = conn.cursor()
read_file = open('tab.txt')

for tablename in read_file:
    col_name = []
    str1 = 'drop table if exists o_' + tablename[0:-1] + ';\n'
    str2 = 'create external table if not exists o_' + tablename + '(\n'
    str0 = str1 + str2
    str3 = ''
    # 获取表注释
    tablename1 = '\'' + tablename[0:-1] + '\''
    tab_comm = cur.execute("show table status where name = %s" % tablename1);
    for comm in cur.fetchmany(tab_comm):
        str3 = comm[-1]
    # 获取表字段及字段类型和注释
    res = cur.execute("show full fields from %s" % tablename)
    for row in cur.fetchmany(res):
        str0 += row[0] + ' ' + row[1] + ' comment \'' + row[-1] + '\',' + '\n'
        col_name.append(row[0])
    str0 = str0[
           0:-2] + '\n' + ') comment ' + '\'' + str3 + '\'\n row format delimited fields terminated by ' + '\'\\001\' stored as textfile location ' + '\'/user/sqoop2/o_' + tablename[
                                                                                                                                                                            0:-1] + '\';\n'
    # 字段类型转换
    str0 = re.sub(r"int\((.*\)) |smallint\((.*\)) |tinyint\((.*\)) ", 'int ', str0)
    str0 = re.sub(r"char\((.*\)) |enum\((.*\)) |timestamp|datetime|varchar\((.*\)) ", 'string ', str0)
    str0 = re.sub(r"float\((.*\)) |decimal\((.*\)) ", 'double ', str0)
    str0 += '-- ' + str(tuple(col_name)) + '\n'
    output = open('/Users/qsq/Desktop/table_data.sql', 'a+')
    output.write(str0)
output.close()
cur.close()
conn.commit()
conn.close()
