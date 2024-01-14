### Query 2 with RDD API ###

import re
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampNTZType, DateType
from pyspark.sql.functions import col, when
import time

start = time.time()

spark = SparkSession \
        .builder \
        .appName("Query 2 RDD") \
        .getOrCreate() \
        .sparkContext

crime_incidents_rdd = spark.textFile("hdfs://okeanos-master:54310/data/crime_incidents_2010-2019.csv,hdfs://okeanos-master:54310/data/crime_incidents_2020-.csv")


def time_of_day(hour):
    if 500 <= hour < 1200:
        return "Morning"
    elif 1200 <= hour < 1700:
        return "Afternoon"
    elif 1700 <= hour < 2100:
        return "Evening"
    else:
        return "Night"

def comma_splits(x):
    return re.split(r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', x)
        
query_2_rdd = crime_incidents_rdd.map(comma_splits) \
        .filter(lambda x: x[15] == "STREET") \
        .map(lambda x: (time_of_day(int(x[3])), 1)) \
        .reduceByKey(lambda x,y: x+y) \
        .sortBy(lambda x: x[1], ascending=False)

print(query_2_rdd.collect())

end = time.time()
print("Query 2 RDD - Execution Time: ",(end-start), "s")
