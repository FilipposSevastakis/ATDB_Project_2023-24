### Query 4: Closest Police Stations ###

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampNTZType, DateType
from pyspark.sql.functions import col, regexp_replace, desc, year, to_date, unix_timestamp, when, udf, count, avg, format_number, row_number
from geopy.distance import geodesic

def get_distance(lat1, long1, lat2, long2):
    return geodesic((lat1, long1), (lat2, long2)).km

get_distance_udf = udf(get_distance, DoubleType())

spark = SparkSession \
        .builder \
        .appName("Query 4: Closest Police Stations") \
        .getOrCreate()

crime_incidents_2010_to_2019_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/crime_incidents_2010-2019.csv")

crime_incidents_2020_to_curr_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/crime_incidents_2020-.csv")

LAPD_Police_Stations_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/LAPD_Police_Stations.csv") \
        .select(
            col("PREC").cast(IntegerType()),
            col("X").cast(DoubleType()),
            col("Y").cast(DoubleType()),
            col("DIVISION").alias("division")
            )


crime_incidents_df = crime_incidents_2010_to_2019_df \
        .union(crime_incidents_2020_to_curr_df) \
        .filter((col("Weapon Used Cd").startswith("1")) & (col("LAT") != "0")) \
        .select(
            col("DR_NO"),
            to_date(unix_timestamp(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a").cast("timestamp"),"yyyy-MM-dd").alias("DATE OCC"),
            col("LAT").cast(DoubleType()),
            col("LON").cast(DoubleType()),
            col("AREA ").cast(IntegerType()).alias("PREC")
            )

# duplicates = crime_incidents_df.groupBy(crime_incidents_df.columns).count().filter(col("count") > 1)
# print(duplicates.count())


crime_x_stations_df = crime_incidents_df.crossJoin(LAPD_Police_Stations_df) \
        .withColumn("year", year("DATE OCC")) \
        .withColumn("distance", get_distance_udf(col("LAT"), col("LON"), col("Y"), col("X")))


closest_window = Window.partitionBy("DR_NO", "LAT", "LON", "year").orderBy("distance")

crime_x_station_df = crime_x_stations_df \
        .withColumn("number_in_part", row_number().over(closest_window)) \
        .filter(col("number_in_part") == 1) 


query_4a_df = crime_x_station_df \
        .groupBy("year").agg(
                format_number(avg(col("distance")), 4).alias("average_distance"),
                count("*").alias("#")
                ) \
        .orderBy(col("year")) \
        .show()

query_4b_df = crime_x_station_df \
        .groupBy("division").agg(
                format_number(avg(col("distance")),4).alias("average_distance"),
                count("*").alias("#")
                ) \
        .orderBy(col("#").desc()) \
        .show(25)

