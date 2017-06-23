#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo
import socket

print(pymongo.version)
print(socket.gethostname())

HOST = socket.gethostname()

GALACTICA = False
LAL = False
WINDOWS = False
ATLAS = False

if HOST == 'mongoserver-1':
    GALACTICA = True
elif HOST == 'vm-75222.lal.in2p3.fr':
    LAL = True
elif HOST == 'nb-arnault3':
    WINDOWS = True
else:
    ATLAS = True

if GALACTICA:
    MONGO_URL = r'mongodb://192.168.56.233:27117'
    HOME = '/home/ubuntu/Spark/mongo/'
    BASE_DATASET = '/mnt/volume/'
elif WINDOWS:
    MONGO_URL = r'mongodb://localhost:27017'
    HOME = '/workspace/LSSTSpark/mongo/'
elif LAL:
    MONGO_URL = r'mongodb://134.158.75.222:27017'
    HOME = '/home/christian.arnault/LSSTSpark/mongo/'
    BASE_DATASET = '/home/christian.arnault/'
elif ATLAS:
    MONGO_URL = r'mongodb://arnault:arnault7977$@cluster0-shard-00-00-wd0pq.mongodb.net:27017,cluster0-shard-00-01-wd0pq.mongodb.net:27017,cluster0-shard-00-02-wd0pq.mongodb.net:27017/<DATABASE>?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'


if __name__ == '__main__':
    pass