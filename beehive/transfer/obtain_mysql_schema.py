# -*- coding: utf-8 -*-

import pymysql
import re
import sys

reload(sys)
sys.setdefaultencoding('utf8')

db_name = "riskdata"
conn = pymysql.connect(host='127.0.0.1', port=3306, user='kfq', passwd='as$h9LxgC1kH', db=db_name, charset='utf8')
cur = conn.cursor()
read_file = open("../configure/%s_tbs.sql" % db_name)

for line in read_file:
    tablename = line.split('=')[0]
    col_name = []
    str1 = 'drop table if exists o_' + tablename + ';\n'
    str2 = 'create external table if not exists o_' + tablename + '(\n'
    str0 = str1 + str2
    str3 = ''
    # 获取表注释
    tab_comm = cur.execute("show table status where name = '%s'" % tablename)
    for comm in cur.fetchmany(tab_comm):
        str3 = comm[-1]
    # 获取表字段及字段类型和注释
    res = cur.execute("show full fields from %s" % tablename)
    for row in cur.fetchmany(res):
        str0 += row[0] + ' ' + row[1] + ' comment \'' + row[-1] + '\',' + '\n'
        col_name.append(row[0])

    # 字段类型转换
    str0 = re.sub(r"int\((.*\)) |smallint\((.*\)) |tinyint\((.*\)) ", 'int ', str0)
    str0 = re.sub(r"char\((.*\)) |enum\((.*\)) |timestamp|datetime|text|varchar\((.*\)) ", 'string ', str0)
    str0 = re.sub(r"float\((.*\)) |decimal\((.*\)) ", 'double ', str0)

    str0 = str0[0:-2] + '\n' + ') comment ' + '\'' + str3 + '\'\n row format delimited fields terminated by ' \
           + '\'\\001\' \nstored as textfile location ' + '\'/user/sqoop2/'+db_name+'/o_' + tablename + '\';\n'

    columns = tablename + '=' + ','.join(col_name) + '\n'

    base_path = "/Users/wangcunxin/temp/output"

    output = open('%s/tb_ddl.sql' % base_path, 'a+')
    output.write(str0)

    output2 = open('%s/tb_columns.sql' % base_path, 'a+')
    output2.write(columns)

output.close()
output2.close()
cur.close()
conn.commit()
conn.close()
