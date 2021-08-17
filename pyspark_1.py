import pandas as pd
import numpy as np
import sys
from operator import add
import pyspark
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <cpus> <output-file-name>")
        sys.exit(0)
    ncpus = sys.argv[1]
    opfile = sys.argv[2]
    spark = SparkSession.builder.master("local["+str(ncpus)+"]").appName('Q1').getOrCreate()
    spark_data = spark.read.csv("airports.csv", inferSchema = True, header = True)
    grp_data = spark_data.groupBy('COUNTRY').count()
    print(grp_data.count())
    grp_data.show()
    presult = grp_data.toPandas()
    presult.to_csv(opfile, header=None, index=None)
    spark.stop()
        