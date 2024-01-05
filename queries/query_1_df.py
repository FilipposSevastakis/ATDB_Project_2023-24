### Query 1 with DataFrame API ###

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampNTZType, DateType
from pyspark.sql.functions import col, to_date, unix_timestamp, year, month, count, row_number

spark = SparkSession \
        .builder \
        .appName("Query 1 DF") \
        .getOrCreate()

crime_incidents_2010_to_2019_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/crime_incidents_2010-2019.csv")

crime_incidents_2020_to_curr_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/crime_incidents_2020-.csv")


crime_incidents_df = crime_incidents_2010_to_2019_df \
        .union(crime_incidents_2020_to_curr_df) \
        .select(
            to_date(unix_timestamp(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a").cast("timestamp"),"yyyy-MM-dd").alias("DATE OCC"),
            )


year_window = Window.partitionBy("year").orderBy(col("count").desc())

query_1_df = crime_incidents_df \
        .withColumn("year", year("DATE OCC")) \
        .withColumn("month", month("DATE OCC")) \
        .groupBy("year", "month").count().alias("crime_total") \
        .sort(col("year")) \
        .withColumn("#", row_number().over(year_window)) \
        .filter(col("#") <= 3)

query_1_df.show()
