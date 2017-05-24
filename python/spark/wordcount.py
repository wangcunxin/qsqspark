# -*- coding:utf-8 -*-
from __future__ import print_function
import os
from operator import add
from pyspark import SparkConf, SparkContext

__author__ = 'kevin'


def filter_special(kv,words):
    ret = True
    if kv[0].strip() in words:
        ret = False
    return ret

if __name__ == '__main__':
    master = "local[2]"
    app_name = "wc"
    input = 'file:/Users/wangcunxin/temp/input/*'

    spark_home = '/Users/wangcunxin/galaxy/spark-2.1.1-bin-hadoop2.6'
    os.environ['SPARK_HOME'] = spark_home

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))
    sc = SparkContext(conf=conf)

    sum = sc.accumulator(0,"my accumulator")
    bcv = sc.broadcast(["etc","and","models","li"])

    lines = sc.textFile(input)

    word_count = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(add).filter(lambda kv:filter_special(kv,bcv.value))
    word_count.foreach(print)

    print(sum.value)

    sc.stop()


