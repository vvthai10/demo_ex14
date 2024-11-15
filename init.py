import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# INIT SPAKR SECSSION
Spark = SparkSession.builder\
    .master("local")\
        .appName("test")\
            .config("spark.driver.bindAddress", "127.0.0.1")\
                .getOrCreate()