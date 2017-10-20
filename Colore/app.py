
from pyspark.sql import SparkSession
from pyspark.sql import functions

from astropy.io import fits
import numpy as np
from pyspark.sql.types import *
import argparse
import random

cores = 100

print("cores = ", cores)

spark = SparkSession\
       .builder\
       .appName("Colore")\
       .config("spark.cores.max", "{}".format(cores))\
       .config("spark.local.dir=/mongo/log/tmp/")\
       .config("spark.executor.memory=20g")\
       .config("spark.storage.memoryFraction=0")\
       .getOrCreate()

sc = spark.sparkContext

hdu = fits.open('/mongo/log/colore/batch/gal10249.fits')

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




