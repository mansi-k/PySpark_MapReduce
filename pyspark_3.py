import numpy as np
import pandas as pd
import sys
from operator import add
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import csv

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <cpus> <output-file-name>")
        sys.exit(0)
    ncpus = sys.argv[1]
    opfile = sys.argv[2]
    spark = SparkSession.builder.master("local["+str(ncpus)+"]").appName('Q3').getOrCreate()
    spark_data = spark.read.csv("airports.csv", inferSchema = True, header = True) [['NAME', 'LATITUDE', 'LONGITUDE']]
    # spark_data.show()
    # print(spark_data.dtypes)
    fltr_data = spark_data.filter((spark_data.LATITUDE<=90) & (spark_data.LATITUDE>=10) & (spark_data.LONGITUDE<=-10) & (spark_data.LONGITUDE>=-90))
    # fltr_data = fltr_data.groupBy('TYPE','NAME','COUNTRY','LATITUDE','LONGITUDE')
    print(fltr_data.count())
    fltr_data.show()
    presult = fltr_data[['NAME']].toPandas()
    presult.to_csv(opfile, header=None, index=None)
    spark.stop()