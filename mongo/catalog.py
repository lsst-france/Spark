#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configure_mongo
import re

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

    with open(configure_mongo.HOME + 'schema.sql', 'rb') as f:
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


if __name__ == '__main__':
    read_schema()

    print('schemas defined')
