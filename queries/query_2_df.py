### Query 2 with DataFrame API ###

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampNTZType, DateType
from pyspark.sql.functions import col, when
import time

start = time.time()

spark = SparkSession \
        .builder \
        .appName("Query 2 DF") \
        .getOrCreate()

crime_incidents_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/crime_incidents.csv") \


crime_incidents_df = crime_incidents_df \
        .select(
            col("TIME OCC").cast("int"),
            col("Premis Desc"),
            )

street_crime_incidents_df = crime_incidents_df.filter(col("Premis Desc") == "STREET")

query_2_df = street_crime_incidents_df \
        .withColumn(
                "time of day",
                when((col("TIME OCC") >= 500) & (col("TIME OCC") < 1200), "Morning") \
                .when((col("TIME OCC") >= 1200) & (col("TIME OCC") < 1700), "Afternoon") \
                .when((col("TIME OCC") >= 1700) & (col("TIME OCC") < 2100), "Evening") \
                .otherwise("Night")
                ) \
        .groupBy("time of day").count() \
        .orderBy(col("count").desc())

query_2_df.show()

end = time.time()
print("Query 2 DF - Execution Time: ",(end-start), "s")
