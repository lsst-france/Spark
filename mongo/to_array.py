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

def do_create(lsst, keys):
    for k in keys:
        if k == '_id':
            continue
        try:
            lsst.drop_collection(k)
            print(k, 'dropped')
        except:
            pass


if __name__ == '__main__':
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst
    dataset = lsst.Object

    count = dataset.count()
    print('Objects', count)


    # result = dataset.find({}, {'ra': 1, 'declVar':1})
    result = dataset.find({}, {'ra':1, 'decl':1, 'raVar':1, 'declVar':1, 'radeclCov':1, 'u_psfFlux':1, 'u_psfFluxSigma':1, 'u_apFlux':1})

    for o in result:
        keys = o.keys()
        do_create(lsst, keys)
        break

    steps = 10000
    step = 100000
    start = 0

    for j in range(steps):

        print('step:', j, 'start:', start)

        dict_group = { k: {'$push': '$' + k }  for k in keys }
        dict_group['_id'] = '$chunkId'

        dict_project1 = { k: {'$ifNull': [ "$" + k, 'None' ]} for k in keys}
        dict_project1['_id'] = 1
        dict_project1['chunkId'] = 1

        dict_project = { k: 1 for k in keys}
        dict_project['_id'] = 1

        skip     = {'$skip': start}
        limit    = {'$limit': step}
        project1 = {'$project': dict_project1}
        group    = {'$group': dict_group }
        project  = {'$project': dict_project }

        print('project1 =', project1)
        print('group =', group)
        print('project =', project)
        print('skip =', skip)
        print('limit =', limit)

        print('=======aggregate')
        cursor = dataset.aggregate( [ skip, limit, project1, group, project ], allowDiskUse=True )

        array_dict = dict()

        for k in keys:
            array_dict[k] = dict() 

        for i, o in enumerate(cursor):
            # print(o)
            chunkId = o['_id']
            for k in keys:
                if k == '_id':
                    continue

                value = []
                if k in o:
                    value = o[k]

                arrays = array_dict[k]

                if chunkId not in arrays:
                    arrays[chunkId] = []

                a = arrays[chunkId]
                a = a + value
                arrays[chunkId] = a

                # print('chunkId=', chunkId, 'k=', k, 'len=', len(value), 'len=', len(a))

                array_dict[k] = arrays

        print('=========== inject array')

        for k in keys:
            arrays = array_dict[k]
            for chunkId in arrays:
                a = arrays[chunkId]

                print('chunkId', chunkId, 'k', k, 'len:', len(a))

                cursor = lsst[k].find( {'_id': chunkId} )

                has = False
                for o in cursor:
                    has = True

                # print('----')

                if not has:
                    print('---------------insert', k, 'len', len(a))
                    lsst[k].insert( {'_id': chunkId, 'v': a} )
                else:
                    print('---------------update', k, 'len', len(a))
                    lsst[k].update( {'_id': chunkId}, { '$push': {'v' : { '$each': a } } } )

        start += step
        if start > count:
            break

"""
    project = {'$project': {'_id':0, 'ra':1} }
    skip = {'$skip': 0}
    limit = {'$limit': steps*step}
    cursor = dataset.aggregate( [ skip, limit, project ], allowDiskUse=True )

    ra0 = []
    for o in cursor:
        ra0.append(o['ra'])

    cursor = lsst['ra'].find( {}, {'_id': 0, 'v': 1} )
    ra1 = []
    for o in cursor:
        ra1 = o['v']
        break

    for i, ra in enumerate(ra0):
        if ra != ra1[i]:
            print(i, ra0[i], ra1[i])
    
    print('ra0', len(ra0), 'ra1', len(ra1), (ra0 == ra1))

"""


