from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampNTZType, DateType
from pyspark.sql.functions import col, to_date, unix_timestamp

spark = SparkSession \
        .builder \
        .appName("DataFrame") \
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
            to_date(unix_timestamp(col("Date Rptd"),"MM/dd/yyyy hh:mm:ss a").cast("timestamp"),"yyyy-MM-dd").alias("Date Rptd"),
            to_date(unix_timestamp(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a").cast("timestamp"),"yyyy-MM-dd").alias("DATE OCC"),
            col('Vict Age').cast(IntegerType()),
            col('LAT').cast(DoubleType()),
            col('LON').cast(DoubleType()),
            )

#crime_incidents_df.show(10)
crime_incidents_df.printSchema()
rows = crime_incidents_df.count()
print(f"DataFrame Rows count : {rows}")
