

from pyspark.sql import SparkSession
from pyspark.sql import functions

import numpy as np
from pyspark.sql.types import *
import random

spark = SparkSession\
       .builder\
       .appName("test")\
       .config("spark.cores.max", "10")\
       .getOrCreate()

sc = spark.sparkContext


# cree deux RDD (K, V)
r1 = sc.parallelize(np.random.rand(100)*10).map(lambda x: int(x)).map(lambda x: (x, 1))
r2 = sc.parallelize(np.random.rand(100)*10).map(lambda x: int(x)).map(lambda x: (x, 1))

# cree un RDD union
r3 = r1.union(r2)

# combinaison des valeurs par key
r1.combineByKey(int, lambda x, y: x+y, lambda x, y: x+y).collect()
r2.combineByKey(int, lambda x, y: x+y, lambda x, y: x+y).collect()
r3.combineByKey(int, lambda x, y: x+y, lambda x, y: x+y).collect()


