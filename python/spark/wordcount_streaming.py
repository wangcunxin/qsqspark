# -*- coding:utf-8 -*-
from operator import add
import os
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

__author__ = 'kevin'

if __name__ == '__main__':
    __separator = ','

    if len(sys.argv) != 3:
        print("Usage: spark_streaming.py <master> <batchInterval>")
        exit(-1)

    master, batchInterval = sys.argv[1:]
    print(master, batchInterval)
    appName = 'kafka_topic_streaming_wc'

    spark_home = '/home/kevin/galaxy/spark-1.6.2-bin-hadoop2.6'
    os.environ['SPARK_HOME'] = spark_home
    sc = SparkContext(master, appName)

    ssc = StreamingContext(sc, int(batchInterval))

    topics = ["topic-wc"]
    brokers = '192.168.2.253:9092'
    kafkaParams = {"metadata.broker.list": brokers}

    kafka_stream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)

    wc = kafka_stream.map(lambda x: x[1]).map(lambda x: x.split(__separator)).map(lambda word: (word, 1))\
        .reduceByKey(add)
    wc.pprint(10)
    def sendPartition(_time, lines):
        print("========= %s =========" % str(_time))
        size = lines.count()
        print(size)
        try:
            if size == 0:
                return
            # wc
            for line in lines:
                print(line)

        except Exception, e:
            print(e)

    wc.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

    ssc.start()
    ssc.awaitTermination()
