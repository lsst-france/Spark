#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import pymongo

# MONGO_URL = r'mongodb://127.0.0.1:27017'
MONGO_URL = r'mongodb://134.158.75.222:27017'

if __name__ == '__main__':
    lines = []

    types = dict()
    fields = dict()

    with open('table.txt', 'rb') as f:
        for line in f:
            line = line.strip().decode('utf-8')
            words = line.split('\t')
            field = words[0]
            ftype = words[1]
            types[ftype] = True
            fields[field] = ftype

    bench = None
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst

    recreate = True

    if recreate:
        try:
            bench = lsst.bench
            lsst.drop_collection('bench')
        except:
            pass

    bench = lsst.bench

    for nobject in range(100):
        obj = dict()

        for field in fields:
            ftype = fields[field]

            value = None

            if ftype == 'bit(1)':
                value = random.randomint(0, 1)
            elif ftype == 'int(11)':
                value = int(random.random())
            elif ftype == 'bigint(20)':
                value = int(random.random())
            elif ftype == 'double':
                value = random.random()

            obj[field] = value

        # object['center'] = {'type': 'Point', 'coordinates': [o.ra, o.dec]}
        try:
            id = bench.insert_one(obj)
            # print('object inserted', o.ra, o.dec)
        except Exception as e:
            print('oops')
            print(e.message)
            sys.exit()

    bench.create_index([('center', '2dsphere')])

