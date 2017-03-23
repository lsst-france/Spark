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

class Schema(object):
    def __init__(self, name):
        self.name = name
        self.fields = dict()
        self.collection = None

    def add_field(self, name, ftype):
        # print('add field', name, ftype)
        self.fields[name] = ftype

    def set_collection(self, col):
        self.collection = col

Schemas = dict()

def read_schema():
    global Schemas

    schema = None
    schema_name = None

    with open('schema.sql', 'rb') as f:
        in_schema = False

        for line in f:
            line = line.strip().decode('utf-8')
            if in_schema:
                if line.startswith(') ENGINE=MyISAM '):
                    Schemas[schema_name] = schema
                    schema = None
                    schema_name = None
                    in_schema = False
                    continue

                words = line.split(' ')

                if len(words) == 0:
                    continue
                if words[0] == 'PRIMARY':
                    continue
                if words[0] == 'KEY':
                    continue

                field = words[0][1:-1]
                ftype = words[1]

                schema.add_field(field, ftype)

            else:
                if line.startswith('CREATE TABLE'):
                    in_schema = True
                    words = line.split(' ')
                    schema_name = words[2][1:-1]
                    print(schema_name)
                    schema = Schema(schema_name)


def read_data(file_name):
    schema = None
    for schema_name in Schemas:
        if file_name.startswith(schema_name):
            schema = Schemas[schema_name]
            break

    if schema is None:
        print('no schema')
        return

    print('using schema', schema_name)

    requests = []

    with open(file_name, 'rb') as f:
        header = True
        line_num = 0
        for line in f:
            line = line.strip().decode('utf-8')
            line_num += 1
            if (line_num % 1000) == 0:
                print(line_num)
            words = line.split(';')
            if header:
                header = False
                fields = words.copy()
                continue

            obj = dict()
            for item, word in enumerate(words):
                field = fields[item]
                if field not in schema.fields:
                    print('error')
                    return

                ftype = schema.fields[field]

                if word == 'NULL':
                    value = None
                else:
                    if ftype == 'bit(1)':
                        value = int(word)
                    elif ftype == 'int(11)':
                        value = int(word)
                    elif ftype == 'bigint(20)':
                        value = int(word)
                    elif ftype == 'double':
                        value = float(word)

                    # print(field, '=', value)
                    obj[field] = value

            requests.append(pymongo.InsertOne(obj))

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
            print(retry, bwe.details)
            continue

if __name__ == '__main__':
    bench = None
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst

    read_schema()

    recreate = True

    if recreate:
        try:
            for schema_name in Schemas:
                lsst.drop_collection(schema_name)
        except:
            pass

    for schema_name in Schemas:
        col = lsst[schema_name]
        schema = Schemas[schema_name]
        schema.set_collection(col)
        Schemas[schema_name] = schema


    for chunk in ('10653', '10056'):
        read_data('Object_{}.csv'.format(chunk))
        read_data('Source_{}.csv'.format(chunk))

