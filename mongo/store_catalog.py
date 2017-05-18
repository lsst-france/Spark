#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configure_mongo

if configure_mongo.LAL:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    from pyspark import SparkConf, SparkContext



import numpy as np

import catalog

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

spark = None

if configure_mongo.LAL:
    spark = SparkSession\
        .builder\
        .appName("AvroKeyInputFormat")\
        .getOrCreate()


def read_images(spark):
    print("========================================= read back data")
    df = spark.read.format("com.databricks.spark.avro").load("./images")
    # df.show()

    rdd = df.rdd.map(lambda x: (x.id, x.r, x.c, np.array(x.image)))

    result = rdd.map(lambda x: analyze(x)).take(1)
    print(result)

if __name__ == '__main__':
    catalog.read_schema()

    print('schemas defined')

    if configure_mongo.LAL:
        sc = spark.sparkContext

    for k in catalog.Schemas:
        print(k)

        schema = catalog.Schemas[k]

        struct_fields = []
        for f in schema.fields:
            field_type = schema.fields[f]
            print(f, field_type)

            t = None
            if field_type == 'bit(1)':
                t = BooleanType()
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

            struct_fields.append(StructField(f, t, True))

"""
        # define the region schema
        image_schema = StructType([StructField("id", IntegerType(), True),
                                   StructField("r", IntegerType(), True),
                                   StructField("c", IntegerType(), True),
                                   StructField("image", ArrayType(ArrayType(DoubleType()), True))])

        if configure_mongo.LAL:
            # create the real regions
            rdd = sc.parallelize(regions).map(lambda x: {'id':x[0], 'r':x[1], 'c':x[2], 'image':np.random.rand(region_size, region_size).tolist()})
            df = spark.createDataFrame(rdd, image_schema)

            # save regions
            df.write.format("com.databricks.spark.avro").mode("overwrite").save("./images")

"""



