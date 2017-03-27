#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, glob
import random
import pymongo
import bson
import decimal
import re
from pymongo.errors import BulkWriteError

MONGO_URL = r'mongodb://127.0.0.1:27017'
# MONGO_URL = r'mongodb://134.158.75.222:27017'
MONGO_URL = r'mongodb://192.168.56.233:27017'

HOME = '/home/ubuntu/Spark/mongo/'
REQUEST_SIZE = 10000

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

class Schema(object):
    def __init__(self, name):
        self.name = name
        self.fields = dict()
        self.collection = None
        self.primary = None

    def add_field(self, name, ftype):
        # print('add field', name, ftype)
        self.fields[name] = ftype

    def set_collection(self, col):
        self.collection = col

    def set_primary(self, primary):
        self.primary = primary.copy()

Schemas = dict()

def read_schema():
    global Schemas

    schema = None
    schema_name = None

    types = dict()
    keys = dict()
    primaries = None

    with open(HOME + 'schema.sql', 'rb') as f:
        in_schema = False

        for line in f:
            line = line.strip().decode('utf-8')
            if in_schema:
                if line.startswith(') ENGINE=MyISAM '):
                    if (primaries is None) and (len(keys) > 0):
                        primaries = [k for k in keys.keys()]

                    print('primary key = ', primaries)
                    # print('keys = ', keys)

                    schema.set_primary(primaries)

                    Schemas[schema_name] = schema
                    schema = None
                    schema_name = None
                    in_schema = False
                    keys = dict()
                    primaries = None

                    continue

                words = line.split(' ')

                if len(words) == 0:
                    continue
                if words[0] == 'PRIMARY':
                    for word in words:
                        if word.startswith('('):
                            word = re.sub('[(]', '', word)
                            word = re.sub('[)][,]', '', word)
                            word = re.sub('[)]', '', word)
                            word = re.sub('[`]', '', word)
                            items = word.split(',')
                            primaries = items
                            # print('line=', line, 'primary=', items, 'word=', word) 
                    continue
                if words[0] == 'KEY':
                    for word in words:
                        if word.startswith('('):
                            word = re.sub('[(]', '', word)
                            word = re.sub('[)][,]', '', word)
                            word = re.sub('[)]', '', word)
                            word = re.sub('[`]', '', word)
                            keys[word] = True
                    continue

                field = words[0][1:-1]
                ftype = words[1]

                types[ftype] = True

                schema.add_field(field, ftype)

            else:
                if line.startswith('CREATE TABLE'):
                    in_schema = True
                    words = line.split(' ')
                    schema_name = words[2][1:-1]
                    print('-------------- starting', schema_name)
                    schema = Schema(schema_name)

    print(types)


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


        # print(field, '=', value)
        obj[field] = value

    col = schema.collection
    if schema.primary is not None:
        col = schema.collection
        query = dict()
        for k in schema.primary:
            query[k] = obj[k]

        result = col.find_one(query, {'_id':1})
        if result is not None:
            # print('object already stored Query=', query)
            return None

        # print('Creating obj')

    return pymongo.InsertOne(obj)

def commit(requests, schema):
    total = 0
    col = schema.collection
    for retry in range(10):
        try:
            result = col.bulk_write(requests)
            total += result.inserted_count
            print(total)
            break
            # print('object inserted')
        except BulkWriteError as bwe:
            print('error in bulk write', retry, bwe.details)
            continue

    return False


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

    if len(requests) > 0:
        status = commit(requests, schema)
    return status

if __name__ == '__main__':
    bench = None
    client = pymongo.MongoClient(MONGO_URL)
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


    class Dataset(object):
        def __init__(self):
            self.schemas = dict()

    datasets = dict()

    p = '/mnt/volume/dataset/'

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
            read_data(p + f)
            break

        break


