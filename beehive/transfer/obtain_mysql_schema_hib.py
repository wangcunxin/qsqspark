# -*- coding: utf-8 -*-

import re
import pymysql

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

dbname = 'paydayloan'
conn = pymysql.connect(host='127.0.0.1', port=3307, user='kfq', passwd='as$h9LxgC1kH', db=dbname,
                       charset='utf8')
cur = conn.cursor()
cur.execute("use %s;" % dbname)
rs = cur.execute("show tables;")
tbs = []
for r in cur.fetchmany(rs):
    tbs.append(r[-1])
print tbs

for tablename in tbs:
    col_name = []
    col_tol = ''
    col_totle = ''
    str1 = 'drop table if exists o_' + tablename + ';\n'
    str2 = 'create external table if not exists o_' + tablename + '(\n'
    str0 = str1 + str2
    str3 = ''
    # 获取表注释
    _sql = "show table status where name = '%s'" % tablename
    tab_comm = cur.execute(_sql);
    for comm in cur.fetchmany(tab_comm):
        str3 = comm[-1]
    # 获取表字段及字段类型和注释
    _sql = "show full fields from %s" % tablename
    res = cur.execute(_sql)
    for row in cur.fetchmany(res):
        str0 += row[0] + ' ' + row[1] + ' comment \'' + row[-1] + '\',' + '\n'
        col_name.append(row[0])

    # 获取列名并拼接
    for col in col_name[1:]:
        col_tol += "cf:" + col + ','
    update_time = '-'
    for col1 in col_name:
        col_totle += col1 + ','

        if col1.find('_update_at') >= 0:
            update_time = col1
        elif col1.find('_create_at') >= 0:
            update_time = col1

    tab_col = tablename + '=' + col_totle[0:-1] + '\n'
    tabs = tablename + '=' + update_time + ',' + col_name[0] + '\n'

    str0 = str0[0:-2] + '\n' + ') comment ' + '\'' + str3 \
           + '\'\n STORED BY ' + '\'org.apache.hadoop.hive.hbase.HBaseStorageHandler\'\n' + \
           'WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,' + col_tol[0:-1] + '")\n' + \
           'TBLPROPERTIES("hbase.table.name"="' + dbname + ':o_' + tablename + '");\n'
    # 字段类型转换
    str0 = re.sub(r"int\((.*\)) |smallint\((.*\)) |tinyint\((.*\)) ", 'int ', str0)
    str0 = re.sub(r"char\((.*\)) |enum\((.*\)) |timestamp|datetime|varchar\((.*\)) ", 'string ', str0)
    str0 = re.sub(r"float\((.*\)) |decimal\((.*\)) ", 'double ', str0)
    # create 'paydayloan:o_address','cf'
    hbs = "create '%s:o_%s','cf'\n" % (dbname,tablename)
    # 输出
    output1 = open('/Users/wangcunxin/temp/output/%s_hib.sql' % dbname, 'a+')
    output1.write(str0)

    output2 = open('/Users/wangcunxin/temp/output/%s_columns.sql' % dbname, 'a+')
    output2.write(tab_col)

    output3 = open('/Users/wangcunxin/temp/output/%s_tbs.sql' % dbname, 'a+')
    output3.write(tabs)

    output4 = open('/Users/wangcunxin/temp/output/%s_hb.sql' % dbname, 'a+')
    output4.write(hbs)


output1.close()
output2.close()
output3.close()
output4.close()
cur.close()
conn.commit()
conn.close()
