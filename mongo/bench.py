#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, glob
import random
import pymongo
import bson
import decimal
import re
from pymongo.errors import BulkWriteError

import configure_mongo

REQUEST_SIZE = 100000

import time
import stepper as st
import catalog

def build_request(words, fields, schema):
    obj = dict()
    for item, word in enumerate(words):
        field = fields[item]

        if field not in schema.fields:
            print('error')
            return None

        ftype = schema.fields[field]

        if word == 'NULL':
            value = None
            continue

        try:
            if ftype == 'bit(1)':
                value = int(word)
            elif ftype == 'int(11)':
                value = int(word)
            elif ftype == 'bigint(20)':
                value = int(word)
            elif ftype == 'double':
                value = float(word)
            elif ftype == 'float':
                value = float(word)
        except:
            # we keep value as a string
            print('field:', field, 'value:', value)
            pass


        # print(field, '=', value)
        obj[field] = value

    return pymongo.InsertOne(obj)

def commit(requests, schema):
    total = 0
    col = schema.collection
    try:
        result = col.bulk_write(requests)
        total += result.inserted_count
        # print(total)
        # print('object inserted')
    except BulkWriteError as bwe:
        # print('error in bulk write', retry, bwe.details)
        pass

    return True


def read_data(file_name):
    schema = None
    name = file_name.split('/')[-1]
    m = re.match('([^_]+)[_]([\d]*)', name)
    try:
        schema_name = m.group(1)
        chunk = m.group(2)
        print(name, schema_name, chunk)
    except:
        print('???', file_name)

    if schema_name in Schemas:
        schema = Schemas[schema_name]

    if schema is None:
        print('no schema')
        return

    print('file', file_name, 'using schema', schema_name)

    requests = []

    with open(file_name, 'rb') as f:
        header = True
        line_num = 0
        for line in f:
            line = line.strip().decode('utf-8')
            line_num += 1
            if (line_num % REQUEST_SIZE) == 0:
                print(line_num)
            words = line.split(';')
            if header:
                header = False
                fields = words.copy()
                continue

            request = build_request(words, fields, schema)
            if request is not None:
                requests.append(request)

            if len(requests) == REQUEST_SIZE:
                status = commit(requests, schema)
                if not status:
                    return

                requests = []                

    status = True

    if len(requests) > 0:
        status = commit(requests, schema)

    return status

if __name__ == '__main__':
    bench = None
    client = pymongo.MongoClient(configure_mongo.MONGO_URL)
    lsst = client.lsst

    read_schema()

    print('schemas defined')

    recreate = False

    if recreate:
        for schema_name in Schemas:
            try:
                print('dropping collection for ', schema_name)
                lsst.drop_collection(schema_name)
            except:
                print('cannot drop collection for ', schema_name)
                pass

    for schema_name in Schemas:
        try:
            print('setting collection', schema_name)
            col = lsst[schema_name]
            schema = Schemas[schema_name]
            schema.set_collection(col)
            Schemas[schema_name] = schema
        except:
            print('cannot set collection', schema_name)

        if schema.primary is not None:
            spec = []
            for k in schema.primary:
                spec.append((k, pymongo.DESCENDING))
            # col.create_index(query, {'unique': True})
            col.create_index(spec, unique=True)

    class Dataset(object):
        def __init__(self):
            self.schemas = dict()


    datasets = dict()

    p = configure_mongo.BASE_DATASET + 'dataset/'
    sav = configure_mongo.BASE_DATASET + 'dataset_save/'

    for file_name in glob.glob(p + '*'):
        name = file_name.split('/')[-1]
        m = re.match('([^_]+)[_](\d+)[.]csv', name)
        try:
            schema = m.group(1)
            chunk = int(m.group(2))
        except:
            continue

        if chunk in datasets:
            ds = datasets[chunk]
            ds.schemas[schema] = True
        else:
            ds = Dataset()
            ds.schemas[schema] = True

        datasets[chunk] = ds

        # print(chunk, sorted(ds.schemas.keys()))



    for chunk in sorted(datasets.keys()):
        ds = datasets[chunk]
        schemas = sorted(ds.schemas.keys())
        fs = ['{}_{}.csv'.format(s, chunk) for s in schemas]
        print('====================== Chunk', chunk, ' => read files:', fs)

        for f in fs:
            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', p + f)
            status = read_data(p + f)
            if status:
                os.rename(p + f, sav + f)


