#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Program to store catalogs available from the LSST dataset into a set of Spark dataframes

Principle:

- the schema from schema.sql is parsed and converted into Spark Structures
- CSV files are read as Dataframes using the schems
- all dssembled dataframes are saved as Avro files partitioned using the chunkId as partition key.

"""



import configure_mongo

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import UserDefinedFunction

import stepper as st

import numpy as np

import catalog
import subprocess
import re


"""
spark-submit --jars $HOME/spark-avro/target/scala-2.11/spark-avro_2.11-3.2.1-SNAPSHOT.jar --executor-memory 12g --conf "spark.kryoserializer.buffer.max=2000mb" avro.py

Documentation Python API

http://spark.apache.org/docs/latest/api/python/pyspark.sql.html

    # conf = SparkConf()
    # conf = conf.set("spark.kryoserializer.buffer.max", "12g")
    # .setAppName("PHOTO")
    # .set("spark.executor.instances", "1").set("spark.executor.cores", 12).set("spark.executor.memory", "16g")
    # sc = SparkContext(conf=conf)

"""

def read_images(spark):
    print("========================================= read back data")
    df = spark.read.format("com.databricks.spark.avro").load("./images")
    # df.show()

    rdd = df.rdd.map(lambda x: (x.id, x.r, x.c, np.array(x.image)))

    result = rdd.map(lambda x: analyze(x)).take(1)
    print(result)


def set_schema_structures():
    for k in catalog.Schemas:
        print(k)

        schema = catalog.Schemas[k]

        struct_fields = []
        for field in schema.fields:
            field_name = field[0]
            field_type = field[1]
            # print(field_name, field_type)

            t = None
            if field_type == 'bit(1)':
                t = IntegerType()
            elif field_type == 'int(11)':
                t = IntegerType()
            elif field_type == 'bigint(20)':
                t = LongType()
            elif field_type == 'double':
                t = DoubleType()
            elif field_type == 'float':
                t = FloatType()
            else:
                t = StringType()

            struct_fields.append(StructField(field_name, t, True))

        struct_schema = StructType(struct_fields)

        schema.set_structure(struct_schema)


def read_data(file_name):
    schema = None
    m = re.match('([^_]+)[_](\d+)[.]csv', file_name)
    try:
        schema_name = m.group(1)
        chunk = m.group(2)
        # print(file_name, schema_name, chunk)
    except:
        print('???', file_name)
        return None

    if schema_name in catalog.Schemas:
        schema = catalog.Schemas[schema_name]

    if schema is None:
        print('no schema')
        return None

    print('file', file_name, 'using schema', schema_name)
    return schema

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

    return obj

def fill_structure(schema, lines):
    header = True
    fields = []
    objects = []
    for line in lines:
        words = line.split(';')
        if header:
            header = False
            fields = words.copy()
            continue

        obj = build_request(words, fields, schema)
        if obj is not None:
            objects.append(obj)

    return objects

if __name__ == '__main__':
    catalog.read_schema()
    set_schema_structures()

    print('schemas defined')

    spark = SparkSession\
        .builder\
        .appName("StoreCatalog")\
        .getOrCreate()

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    total = None

    file_number = 0

    stepper = st.Stepper()

    first = True

    cat = subprocess.Popen(["hadoop", "fs", "-ls", "/user/christian.arnault/swift"], stdout=subprocess.PIPE)
    for file_number, line in enumerate(cat.stdout):
        line = line.strip().decode('utf-8')
        file_name = line.split('/')[-1].strip()
        schema = read_data(file_name)
        if schema is None:
           continue

        df = sqlContext.read \
          .format('com.databricks.spark.csv') \
          .options(header='true', delimiter=';') \
          .load('swift/' + file_name, schema = schema.structure)

        # schema = schema.structure
        # inferSchema = True

        # result = df.select('declVar', 'z_flagBadApFlux').show()
        # print(result)

        """
        if total is not None:
            total = total.union(df)
        else:
            total = df
        """

        stepper.show_step('read file number {}'.format(file_number))

        write_mode = 'append'
        if first:
            write_mode = 'overwrite'
            first = False

        df.write.format("com.databricks.spark.avro").mode(write_mode).partitionBy('chunkId').save("./lsstdb")
        stepper.show_step('database written')

        """
        #df.printSchema()
        #print(df.schema)
        """

    stepper = st.Stepper()
    total = sqlContext.read.format("com.databricks.spark.avro").load("./lsstdb")
    stepper.show_step('database written')

    result = total.filter("deepSourceId = 2322374716295173").select('ra', 'decl').show()
    stepper.show_step('SELECT ra, decl FROM Object WHERE deepSourceId = 2322374716295173')

    result = total.select('chunkId').show()
    print(result)

    sc.stop()



