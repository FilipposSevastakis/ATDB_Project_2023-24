### Query 1 with SQL API ###

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampNTZType, DateType
from pyspark.sql.functions import col, to_date, unix_timestamp, year, month
import time

start = time.time()

spark = SparkSession \
        .builder \
        .appName("Query 1 SQL") \
        .getOrCreate()

crime_incidents_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/crime_incidents.csv")

crime_incidents_df = crime_incidents_df \
        .select(
            to_date(unix_timestamp(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a").cast("timestamp"),"yyyy-MM-dd").alias("date_occ"),
            )


crime_incidents_df.createOrReplaceTempView("crime_incidents")

query_1 = "SELECT year, month, crime_total, rank FROM (SELECT year(date_occ) AS year, month(date_occ) AS month, count(*) AS crime_total, RANK() OVER (PARTITION BY year(date_occ) ORDER BY count(*) DESC) AS rank FROM crime_incidents GROUP BY year, month) ranked WHERE rank <=3  ORDER BY year, rank"

spark.sql(query_1).show(45)

end = time.time()
print("Query 1 SQL - Execution Time: ",(end-start), "s")
