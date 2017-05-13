#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, glob
import random
import pymongo
import bson
import decimal
import re
import argparse
from bson.objectid import ObjectId

from pymongo.errors import BulkWriteError

import stepper as st

import socket
print(socket.gethostname())

HOST = socket.gethostname()

if HOST == 'mongoserver-1':
    GALACTICA = True
elif HOST == 'vm-75222.lal.in2p3.fr':
    LAL = True
elif HOST == 'nb-arnault3':
    WINDOWS = True
else:
    ATLAS = True

GALACTICA = True

if GALACTICA:
    # MONGO_URL = r'mongodb://192.168.56.233:27117'
    MONGO_URL = r'mongodb://193.55.95.149:27117'
elif WINDOWS:
    MONGO_URL = r'mongodb://localhost:27017'
elif LAL:
    MONGO_URL = r'mongodb://134.158.75.222:27017'
elif ATLAS:
    MONGO_URL = r'mongodb://arnault:arnault7977$@cluster0-shard-00-00-wd0pq.mongodb.net:27017,cluster0-shard-00-01-wd0pq.mongodb.net:27017,cluster0-shard-00-02-wd0pq.mongodb.net:27017/<DATABASE>?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'

print(pymongo.version)

"""
SELECT ra, decl, raVar, declVar, radeclCov, u_psfFlux, u_psfFluxSigma, u_apFlux FROM Object WHERE deepSourceId = 2322920177140010;
"""

if __name__ == '__main__':
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst
    dataset = lsst.Object

    result = dataset.find({}, {'ra':1, 'decl':1, 'raVar':1, 'declVar':1, 'radeclCov':1, 'u_psfFlux':1, 'u_psfFluxSigma':1, 'u_apFlux':1})

    for i, o in enumerate(result):
        print(i, o)
        if i > 10:
            break
