
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark import StorageLevel

import os
from astropy.io import fits
import numpy as np
from pyspark.sql.types import *
import argparse
import random

import stepper as stp

cores = 1
tmp = '/'
where = '/'

if os.path.exists('/mongo'):
    where = '/mongo/log/colore/batch/'
    cores = 100
    tmp = '/mongo/log/tmp/'
elif os.path.exists('/home/ubuntu/'):
    where = '/home/ubuntu/'
    cores = 8
    tmp = '/home/ubuntu/'
else:
    print('where can I get fits files?')
    exit()

parser = argparse.ArgumentParser()
parser.add_argument('-subset', type=int, default=100, help='subset of the data from the FITS file')
parser.add_argument('-executor', default='1g', help='executor memory')
parser.add_argument('-driver', default='1g', help='driver memory')
parser.add_argument('-partitions', type=int, default='1000', help='partitions')
parser.add_argument('-coalesce', type=int, default='1000', help='coalesce')
parser.add_argument('-steps', type=int, default='20', help='steps')
parser.add_argument('-message', default='200m', help='message')
parser.add_argument('-output', default='colore', help='output data area')
parser.add_argument('-cores', type=int, default='{}'.format(cores), help='cores')


args = parser.parse_args()

print("cores = ", cores)

print("subset = ", args.subset)
print("executor = ", args.executor)
print("driver = ", args.driver)
print("partitions = ", args.partitions)
print("coalesce = ", args.coalesce)
print("steps = ", args.steps)
print("message = ", args.message)
print("output = ", args.output)
print("cores = ", args.cores)

# exit()

# 148 210 556
# 134 217 728

spark = SparkSession\
       .builder\
       .appName("Colore")\
       .config("spark.local.dir={}".format(tmp))\
       .config("spark.rpc.message.maxSize:{}".format(args.message)) \
       .config("spark.executor.memory={}".format(args.executor))\
       .config("spark.driver.memory={}".format(args.driver)) \
       .config("spark.cores.max", "{}".format(args.cores)) \
    .getOrCreate()


"""

       .config("spark.memory.fraction=0.8")\
       .config("spark.storage.memoryFraction=0")\

       
"""

# df.write.mode("overwrite").save("./images")

sc = spark.sparkContext

hdu = fits.open(where + 'gal10249.fits')

hdu.info()
hdu[0].header
hdu[1].data

header = hdu[1].header
rows = header['NAXIS2']

fields = dict()

for i in range(header['TFIELDS']):
    j = i + 1
    fields[header['TTYPE{}'.format(j)]] = (header['TFORM{}'.format(j)], header['TUNIT{}'.format(j)])

print("Fields = ", fields)



for row in range(10):
    print(hdu[1].data[row])

data = hdu[1].data

s1 = stp.Stepper()
s = stp.Stepper()

"""
Transpose the data table columns => rows
Work upon a "subset" = % of the full data table
And apply the transposition by blocks ("steps" = # blocks)
"""

def build(sc, data, subset, steps):
    # will create a list of transposed arrays
    if subset <= 0:
        return None

    if subset > 100:
        subset = 100

    used_data_size = int(data.size * subset / 100.0)

    points = None

    block = int(used_data_size / steps)

    print("steps = ", steps, " size = ", used_data_size, " block = ", block, " total data = ", data.size)

    for i in range(steps):
        start = int(i * block)
        ra = data['RA'][start:start+block]
        dec = data['DEC'][start:start+block]
        z = data['Z_COSMO'][start:start+block]
        dz = data['DZ_RSD'][start:start+block]

        p = np.column_stack((ra, dec, z, dz))

        # build a RDD from the transposed data.
        r = sc.parallelize(p, args.partitions).map(lambda x: (float(x[0]), float(x[1]), float(x[2]), float(x[3])))
        s.show_step("==> parallelize")

        if points is None:
            points = r
        else:
            points = points.union(r)

        # points.append(np.column_stack((ra, dec, z, dz)))

        s.show_step("==> i={}".format(i))

    return points
    # return np.concatenate(points)
#=============================================

allp = build(sc, data, subset=args.subset, steps=args.steps)

s1.show_step("==> total")

# lower the patitioning
rdd = allp.coalesce(args.coalesce)

s1.show_step("==> coalesce")

# make it a dataframe
df = rdd.toDF(['RA', 'DEC', 'Z', 'DZ'])

s1.show_step("==> dataframe")

"""
# size = sc._jvm.org.apache.spark.util.SizeEstimator.estimate(rdd)

df.persist(StorageLevel.MEMORY_ONLY)
size = df.count()
df.unpersist()

print("size = ", size)
"""

print(df.show(10))

# save the result
df.write.mode("overwrite").save("./{}".format(args.output))

s1.show_step('write data')


def get_minmax(allp):
    s2 = stp.Stepper()
    minmax = allp.agg(F.min('RA'), F.max('RA'), F.min('DEC'), F.max('DEC')).collect()
    s2.show_step("get min-max")
    print(minmax)




