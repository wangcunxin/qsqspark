# -*- coding:utf-8 -*-
from __future__ import print_function

import os
from pyspark import SparkContext, SparkConf

__author__ = 'kevin'

if __name__ == '__main__':

    master = "local[2]"
    app_name = "spark_sql_test"

    spark_home = '/home/kevin/galaxy/spark-1.6.2-bin-hadoop2.6'
    os.environ['SPARK_HOME'] = spark_home

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))

    sc = SparkContext(conf=conf)

    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
    host = "192.168.2.253"
    table = "trades"
    confConfig = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
    hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=confConfig)

    output = hbase_rdd.collect()
    for (k, v) in output:
        print((k, v))

    sc.stop()
