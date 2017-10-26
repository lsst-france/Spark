
from pyspark.sql import SparkSession
from pyspark.sql import functions

from astropy.io import fits
import numpy as np
from pyspark.sql.types import *
import argparse
import random

import stepper as stp

cores = 100

print("cores = ", cores)

spark = SparkSession\
       .builder\
       .appName("Colore")\
       .config("spark.cores.max", "{}".format(cores))\
       .config("spark.executor.memory=20g") \
       .getOrCreate()

       # .config("spark.local.dir=/mongo/log/tmp/")\
       # .config("spark.storage.memoryFraction=0")\

sc = spark.sparkContext

where = '/home/ubuntu/'
where = '/mongo/log/colore/batch/'

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

dfs = []

s1 = stp.Stepper()
s = stp.Stepper()

subset = 1 
steps = 100
part = subset / steps
block = int((data.size / subset) / steps)

print("steps = ", steps, " part = ", part, " block = ", block, " total data = ", (block * steps))

for i in range(steps):
  start = int(i * block)
  ra = data['RA'][start:start+block]
  dec = data['DEC'][start:start+block]
  z = data['Z_COSMO'][start:start+block]
  dz = data['DZ_RSD'][start:start+block]

  points = np.column_stack((ra, dec, z, dz))

  dfs.append(sc.parallelize(points, 10).map(lambda x: (float(x[0]), float(x[1]), float(x[2]), float(x[3]))))

  # print("i=", i, " points=", points_df.take(10))
  s.show_step("==> i={}".format(i))

allp = sc.union(dfs).toDF(['RA', 'DEC', 'Z', 'DZ'])

s1.show_step("==> total")

print(allp.show(10))

maxra = allp.agg({'RA' : "max"}).collect()[0]
minra = allp.agg({'RA' : "min"}).collect()[0]
maxdec = allp.agg({'DEC' : "max"}).collect()[0]
mindec = allp.agg({'DEC' : "min"}).collect()[0]


print(maxra, minra, maxdec, mindec)


