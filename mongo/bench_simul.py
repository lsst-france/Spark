#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import pymongo
import bson
import decimal
from pymongo.errors import BulkWriteError

MONGO_URL = r'mongodb://127.0.0.1:27017'
# MONGO_URL = r'mongodb://134.158.75.222:27017'

import time

class Stepper(object):
    previous_time = None

    def __init__(self):
        self.previous_time = time.time()

    def show_step(self, label='Initial time'):
        now = time.time()
        delta = now - self.previous_time

        print('--------------------------------', label, '{:.3f} seconds'.format(delta))

        self.previous_time = now


def creation(bench):
    stepper = Stepper()

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

    stepper.show_step('config data read')

    deepSourceId = 2322920000000000

    total = 0

    stepper2 = Stepper()

    for step in range(1):
        requests = []
        for nobject in range(10000):
            obj = dict()

            for field in fields:
                ftype = fields[field]

                value = None

                if ftype == 'bit(1)':
                    value = int(random.random()*2)
                elif ftype == 'int(11)':
                    value = int(random.random()*100000000000)
                elif ftype == 'bigint(20)':
                    if field == 'deepSourceId':
                        value = deepSourceId
                    else:
                        value = int(random.random()*100000000000.0)
                elif ftype == 'double':
                    if field == 'ra':
                        value = random.random()*180. - 90.
                    elif field == 'decl':
                        value = random.random()*90.
                    else:
                        value = random.random()

                # print(field, value)
                obj[field] = value

            deepSourceId += 1

            obj['center'] = {'type': 'Point', 'coordinates': [obj['ra'], obj['decl']]}
            obj['loc'] = [ obj['ra'], obj['decl'] ]

            requests.append(pymongo.InsertOne(obj))

        for retry in range(10):
            try:
                result = bench.bulk_write(requests)
                total += result.inserted_count
                print(total)
                break
                # print('object inserted')
            except BulkWriteError as bwe:
                pprint(retry, bwe.details)
                continue

        stepper2.show_step('bulk ingestion done')

    stepper.show_step('all ingestion done')

    bench.create_index([('center', '2dsphere')])

    stepper.show_step('index created')

    bench.create_index([('loc', '2d')])

    stepper.show_step('2D index')



if __name__ == '__main__':

    bench = None
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst

    recreate = False

    if recreate:
        try:
            lsst.drop_collection('bench')
        except:
            pass

        bench = lsst.bench
        creation(bench)

    else:

        bench = lsst.bench

    stepper = Stepper()

    ra = -45.4
    decl = 66.
    radius = 0.005

    ext = 2.
    bottomleft = [ ra - ext, decl - ext ]
    topright = [ ra + ext, decl + ext ]

    box = True

    if box:
        result = bench.find({'loc': {'$geoWithin': {'$box': [ bottomleft, topright ]}}},
                            {'_id': 0, 'where': 1, 'center': 1, 'deepSourceId': 1, 'ra': 1, 'decl': 1})
    else:
        result = bench.find({'center': {'$geoWithin': {'$centerSphere': [[ra, decl], radius]}}},
                            {'_id': 0, 'where': 1, 'center': 1, 'deepSourceId': 1, 'ra': 1, 'decl': 1})

    count = result.count()

    for oid, o in enumerate(result):
        print(o['deepSourceId'], o['ra'], o['decl'])
        pass

    stepper.show_step('Find {}'.format(count))

    pipeline = [
        {'$limit': 10},
        {'$project': {'_id': 1, 'deepSourceId': 1, 'ra': 1, 'decl': 1}},
        {'$lookup': {'from': 'bench', 'localField': '_id', 'foreignField': '_id', 'as': 'couples'}},
    ]

    result = bench.aggregate(pipeline)
    for oid, o in enumerate(result):
        print(o['deepSourceId'], o['couples'][0]['deepSourceId'])

