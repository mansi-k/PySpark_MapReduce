import pandas as pd
import numpy as np
import sys
from operator import add
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <cpus> <output-file-name>")
        sys.exit(0)
    ncpus = sys.argv[1]
    opfile = sys.argv[2]
    spark = SparkSession.builder.master("local["+str(ncpus)+"]").appName('Q2').getOrCreate()
    spark_data = spark.read.csv("airports.csv", inferSchema = True, header = True)
    grp_data = spark_data.groupBy('COUNTRY').count().sort(col("count").desc())
    # grp_data.show()
    presult = grp_data.toPandas().head(1)
    print(presult)
    presult.to_csv(opfile, header=None, index=None)
    spark.stop()
        