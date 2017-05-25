# -*- coding:utf-8 -*-
from __future__ import print_function
import os
from pyspark.sql.types import *
from beehive.utils.date_util import DateUtil
from pyspark.sql import SparkSession

__author__ = 'kevin'


if __name__ == '__main__':

    app_name = "wc"
    input = 'file:/Users/wangcunxin/temp/input/*'

    spark_home = '/Users/wangcunxin/galaxy/spark-2.1.1-bin-hadoop2.6'
    os.environ['SPARK_HOME'] = spark_home

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()
    sc = spark.sparkContext

    lines = sc.textFile(input)
    users = lines.map(lambda x: x[1].split(' ')).filter(lambda x: len(x) == 2) \
        .map(lambda p: (p[0], p[1].strip()))

    schema_string = "firstname lastname"
    fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split(' ')]
    schema = StructType(fields)
    schema_users = spark.createDataFrame(users, schema)
    schema_users.createOrReplaceTempView("user")

    # regist udf
    #spark.registerFunction("get_date", lambda x: DateUtil.str_to_date(x).date(), DateType())
    #spark.registerFunction("date_diff", lambda x, k: DateUtil.date_diff(x, k), IntegerType())
    #spark.registerFunction("get_hour", lambda x: DateUtil.str_to_date(x).hour(), IntegerType())
    #spark.registerFunction("to_int", lambda x: int(x), IntegerType())
    #spark.registerFunction("timestamp_diff", lambda x, k: DateUtil.timestamp_diff(x, k), IntegerType())

    _sql = "select firstname,lastname from user"
    rs = spark.sql(_sql)
    rs.show()
    lines_list = []
    sep = "\t"
    for r in rs.collect():
        if r != None and len(r) > 0:
            tmp = []
            for t in r:
                tmp.append(str(t))
            line = sep.join(tmp)
            lines_list.append(line)

    output = ""
    print(lines_list)
    # write to file
    #self._write_file(lines_list, output)

    sc.stop()
