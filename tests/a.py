
"""
spark-submit --conf spark.kryoserializer.buffer.max=2000mb --executor-memory 20g --total-executor-cores 10 a.py -c -b 1000000 -r 1000
"""

from pyspark.sql import SparkSession

import numpy as np
from pyspark.sql.types import *
import argparse
import random

spark = SparkSession\
       .builder\
       .appName("test")\
       .config("spark.cores.max", "10")\
       .getOrCreate()

sc = spark.sparkContext

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--create', action="store_true")
parser.add_argument('-r', '--records', type=int, default=1000)
parser.add_argument('-b', '--block', type=int, default=1000)
parser.add_argument('-s', '--steps', type=int, default=10)
args = parser.parse_args()

create = args.create

runs = 10
records = args.records
block = args.block
steps = args.steps

schema = StructType([StructField("run", IntegerType(), True),
                     StructField("image", ArrayType(DoubleType(), True))])

if create:
    print('creating data with', records, 'records made of blocks of', block, 'doubles')
    rdd = sc.parallelize(range(runs*records), 30).map(lambda x: (int(random.random()*runs), np.random.rand(block).tolist()))
    df = spark.createDataFrame(rdd, schema)
    df.write.mode("overwrite").save("./images")
else:
    print('reading data and applying', steps, 'steps to them')
    df = spark.read.load("./images")
    df = df.filter(df.run == 3)
    rdd = df.rdd.map(lambda x : (x.run, np.array(x.image)))

    for step in range(steps):
        rdd = rdd.map(lambda x: (x[0], np.exp(x[1])))
        rdd = rdd.map(lambda x: (x[0], np.log(x[1])))
        if (step % 10) == 0:
            rdd = rdd.cache()

    rdd = rdd.map(lambda x: (x[0], np.sum(x[1])))
    result = rdd.take(10)

    print(result)

