# -*- coding:utf8 -*-
from hbase.ttypes import Mutation, BatchMutation
from thrift.transport import TTransport
from thrift.transport.TSocket import TSocket
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from hbase import Hbase

__author__ = 'wangcx'


class HBaseUtil:
    def __init__(self):
        pass

    @staticmethod
    def getBatchMutations(cf, qualifiers, tups):
        batchMutations = []
        for tup in tups:
            mutations = []
            for i in range(1, len(qualifiers)):
                mutation = Mutation(column="%s:%s" % (cf, qualifiers[i]), value=tup[i])
                mutations.append(mutation)
            batchMutation = BatchMutation(tup[0], mutations)
            batchMutations.append(batchMutation)
        return batchMutations


class HBaseClient:
    def __init__(self):
        # self.transport = TTransport.TFramedTransport(TSocket("192.168.2.254", 9090))
        self.transport = TTransport.TBufferedTransport(TSocket("192.168.2.254", 9090), 10 * 1024 * 1024)
        self.transport.open()
        # self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)

    def __del__(self):
        self.transport.close()

    def list_table(self):
        for table in self.client.getTableNames():
            print table

    def get(self, tableName, rowkey):
        rs = self.client.getRow(tableName, rowkey)
        return rs

    def scanByPrefix(self, table, prefix, cfs):
        rets = []
        try:
            scanner = self.client.scannerOpenWithPrefix(tableName=table, startAndPrefix=prefix, columns=cfs)
            try:
                size = 10000
                rs = self.client.scannerGetList(scanner, size)
                while rs:
                    rets.extend(rs)
                    rs = self.client.scannerGetList(scanner, size)
                    print size
            finally:
                self.client.scannerClose(scanner)
        except Exception, e:
            print e
        return rets

    def scan(self, table, cfs):
        rets = []
        try:
            scanner = self.client.scannerOpen(tableName=table, startRow="", columns=cfs)
            try:
                size = 10000
                rs = self.client.scannerGetList(scanner, size)
                while rs:
                    rets.extend(rs)
                    rs = self.client.scannerGetList(scanner, size)
            finally:
                self.client.scannerClose(scanner)
        except Exception, e:
            print e
        return rets

    def insertOne(self, tableName, rk, kvs):
        mutations = []
        for col, val in kvs:
            # mutations = [Mutation(column="cf:a", value="1")]
            mut = Mutation(column=col, value=val)
            mutations.append(mut)
        self.client.mutateRow(tableName, rk, mutations)

    def insertMany(self, tableName, batchMutations):
        size = len(batchMutations)
        if (size > 2000):
            batch = []
            for i in range(0, size):
                batch.append(batchMutations[i])
                if (i % 2000 == 0):
                    self.client.mutateRows(tableName, batch)
                    batch = []
        else:
            self.client.mutateRows(tableName, batchMutations)
