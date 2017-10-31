
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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


print("cores = ", cores)


spark = SparkSession\
       .builder\
       .appName("Colore")\
       .config("spark.local.dir={}".format(tmp))\
       .config("spark.memory.fraction=0.8")\
       .getOrCreate()


"""

       .config("spark.memory.fraction=0.8")\
       .config("spark.storage.memoryFraction=0")\

       .config("spark.cores.max", "{}".format(cores))\
       .config("spark.executor.memory=20g") \
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
def build(data, subset, steps):
  # will create a list of transposed arrays
  points = []
  subset = int(1.0 / subset)

  if subset <= 0:
      return None

  part = subset / steps
  block = int((data.size / subset) / steps)

  print("steps = ", steps, " part = ", part, " block = ", block, " total data = ", (block * steps))


  for i in range(steps):
    start = int(i * block)
    ra = data['RA'][start:start+block]
    dec = data['DEC'][start:start+block]
    z = data['Z_COSMO'][start:start+block]
    dz = data['DZ_RSD'][start:start+block]

    points.append(np.column_stack((ra, dec, z, dz)))

    s.show_step("==> i={}".format(i))

  return np.concatenate(points)
#=============================================

allp = build(data, subset=1.0, steps=20)

s1.show_step("==> total")

# build a RDD from the transposed data. 
rdd = sc.parallelize(allp, 100000).map(lambda x: (float(x[0]), float(x[1]), float(x[2]), float(x[3])))

s1.show_step("==> parallelize")

# lower the patitioning
rdd = rdd.coalesce(100)

s1.show_step("==> coalesce")

# make it a dataframe
df = rdd.toDF(['RA', 'DEC', 'Z', 'DZ'])

s1.show_step("==> dataframe")

print(df.show(10))

# save the result
df.write.mode("overwrite").save("./colore")

s1.show_step('write data')


def get_minmax(allp):
    s2 = stp.Stepper()
    minmax = allp.agg(F.min('RA'), F.max('RA'), F.min('DEC'), F.max('DEC')).collect()
    s2.show_step("get min-max")
    print(minmax)




