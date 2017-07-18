
"""
spark-submit --conf spark.kryoserializer.buffer.max=2000mb --executor-memory 20g --total-executor-cores 10 a.py -c -b 1000000 -r 1000
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions

import numpy as np
from pyspark.sql.types import *
import argparse
import random
import stepper as st

cores = 10

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--create', action="store_true")
parser.add_argument('-r', '--records', type=int, default=1000)
parser.add_argument('-b', '--block', type=int, default=1000)
parser.add_argument('-s', '--steps', type=int, default=10)
parser.add_argument('-k', '--cores', type=int, default=10)
args = parser.parse_args()

cores = args.cores

print("cores = ", cores)

spark = SparkSession\
       .builder\
       .appName("test")\
       .config("spark.cores.max", "{}".format(cores))\
       .config("spark.local.dir=/mongo/log/tmp/")\
       .config("spark.executor.memory=20g")\
       .config("spark.storage.memoryFraction=0")\
       .getOrCreate()

sc = spark.sparkContext

create = args.create

runs = 10
records = args.records
block = args.block
steps = args.steps

schema = StructType([StructField("run", IntegerType(), True),
                     StructField("image", ArrayType(DoubleType(), True))])

partitions = 10000  # to have 256Mb files
if create:
    print('creating data with', records, 'records made of blocks of', block, 'doubles')
    stepper = st.Stepper()
    rdd = sc.parallelize(range(runs*records), partitions).map(lambda x: (int(random.random()*runs), np.random.rand(block).tolist()))
    stepper.show_step('create data')
    df = spark.createDataFrame(rdd, schema)
    stepper.show_step('create dataframe')
    # input("ok")
    df.write.mode("overwrite").save("./images")
    stepper.show_step('write data')
else:
    stepper = st.Stepper()
    print('reading data and applying', steps, 'steps to them')
    df = spark.read.load("./images")
    stepper.show_step('read data')

    df = df.filter(df.run == 3)
    stepper.show_step('filter')

    exp = functions.udf(lambda m: np.exp(np.array(m)).tolist(), ArrayType(DoubleType(), True))
    log = functions.udf(lambda m: np.log(np.array(m)).tolist(), ArrayType(DoubleType(), True))
    stepper.show_step('step0')

    msum = functions.udf(lambda m: float(np.sum(np.array(m))), DoubleType())
    size = functions.udf(lambda m: len(m), IntegerType())

    for step in range(steps):
        df = df.select(df.run, exp(df.image).alias('image'))
        df = df.select(df.run, log(df.image).alias('image'))

    stepper.show_step('steps')

    count = df.count()
    print("count=", count)

    df1 = df.select(df.run, msum(df.image).alias('sum'))
    total = df1.rdd.map(lambda x: float(x["sum"])).reduce(lambda x, y: x+y)
    stepper.show_step('sum')
    print('total=', total)

    df2 = df.select(df.run, size(df.image).alias('size'))
    stepper.show_step('size')
    df2.rdd.take(10)


