
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import numpy as np

"""
spark-submit --jars target/scala-2.11/spark-avro_2.11-3.2.1-SNAPSHOT.jar a.py

Documentation Python API
http://spark.apache.org/docs/latest/api/python/pyspark.sql.html
"""

spark = SparkSession\
        .builder\
        .appName("AvroKeyInputFormat")\
        .getOrCreate()


def create_images(spark):

    # conf = SparkConf()
    # conf = conf.set("spark.kryoserializer.buffer.max", "12g")
    # .setAppName("PHOTO")
    # .set("spark.executor.instances", "1").set("spark.executor.cores", 12).set("spark.executor.memory", "16g")
    # sc = SparkContext(conf=conf)

    sc = spark.sparkContext


    rows = 3
    cols = 3
    regions = []
    region_id = 0
    region_size = 4000

    # initialize region descriptors
    for r in range(rows):
        for c in range(cols):
            regions.append((region_id, r, c))
            region_id += 1


    # define the region schema
    image_schema = StructType([StructField("id", IntegerType(), True),
                               StructField("r", IntegerType(), True),
                               StructField("c", IntegerType(), True),
                               StructField("image", ArrayType(ArrayType(DoubleType()), True))])

    # create the real regions
    rdd = sc.parallelize(regions).map(lambda x: {'id':x[0], 'r':x[1], 'c':x[2], 'image':np.random.rand(region_size, region_size).tolist()})
    df = spark.createDataFrame(rdd, image_schema)

    # save regions
    df.write.format("com.databricks.spark.avro").mode("overwrite").save("./images")

def read_images(spark):
    print("========================================= read back data")
    df = spark.read.format("com.databricks.spark.avro").load("./images")
    # df.show()

    rdd = df.rdd.map(lambda x: np.array(x.image))

    print(rdd.take(2))


read_images(spark)



