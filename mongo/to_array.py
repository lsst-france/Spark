#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
Program to convert the row oriented Mongo collections into collections containing arrays.

Apparently this appears difficult !!!

- one cannot aggregate une single array (10^8 elements)


"""


import os, glob
import random
import pymongo
import bson
import decimal
import re
import argparse
from bson.objectid import ObjectId
from urllib.parse import quote_plus

from pymongo.errors import BulkWriteError

import stepper as st

import configure_mongo

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
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--fields', default="")
    parser.add_argument('-u', '--user', default="")
    parser.add_argument('-p', '--password', default="")

    args = parser.parse_args()

    if args.fields == "":
        print('no fields')
        exit()

    print('fields', args.fields)

    uri = configure_mongo.MONGO_URL
    if args.password != "":
        ip = re.sub('mongodb://', '', configure_mongo.MONGO_URL)
        uri = "mongodb://%s:%s@%s" % ( quote_plus('lsst'), quote_plus(args.password), ip)
        print(uri)

    client = pymongo.MongoClient(uri)
    lsst = client.lsst
    dataset = lsst.Object

    count = dataset.count()
    print('Objects', count)

    exit()

    fields = args.fields.split(',')

    view = {k: 1 for k in fields}
    print(view)
    result = dataset.find({}, view)

    for o in result:
        keys = o.keys()
        do_create(lsst, keys)
        break

    steps = 10000
    step = 100000
    start = 0

    stepper = st.Stepper()

    for j in range(steps):

        stepper.show_step('step start={}'.format(start))

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

        if j == 0:
            print('project1 =', project1)
            print('group =', group)
            print('project =', project)
            print('skip =', skip)
            print('limit =', limit)

        # print('=======aggregate')
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

        # print('=========== inject array')

        for k in keys:
            arrays = array_dict[k]
            for chunkId in arrays:
                a = arrays[chunkId]

                # print('chunkId', chunkId, 'k', k, 'len:', len(a))

                cursor = lsst[k].find( {'_id': chunkId} )

                has = False
                for o in cursor:
                    has = True

                # print('----')

                if not has:
                    # print('---------------insert', k, 'len', len(a))
                    lsst[k].insert( {'_id': chunkId, 'v': a} )
                else:
                    # print('---------------update', k, 'len', len(a))
                    lsst[k].update( {'_id': chunkId}, { '$push': {'v' : { '$each': a } } } )

        start += step
        if start > count:
            break


