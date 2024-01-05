### Query 3 ###

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampNTZType, DateType
from pyspark.sql.functions import col, split, regexp_replace, desc, year, to_date, unix_timestamp, when

spark = SparkSession \
        .builder \
        .appName("Query 3") \
        .getOrCreate()

crime_incidents_2010_to_2019_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/crime_incidents_2010-2019.csv")

LA_income_2015_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/LA_income_2015.csv")

rev_geocoding_df = spark.read.format('csv') \
        .options(header = True, inferSchema = True) \
        .load("hdfs://okeanos-master:54310/data/revgecoding.csv")

rev_geocoding_unique_df = rev_geocoding_df.withColumn("ZIPcode", split(col("ZIPcode"), ";").getItem(0))\

crime_incidents_2015_df = crime_incidents_2010_to_2019_df.select(col("Vict Descent"), col("LAT"), col("LON"), col("DATE OCC")) \
        .withColumn("year", year(to_date(unix_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a").cast("timestamp"),"yyyy-MM-dd"))) \
        .filter((col("year") == 2015) & (col("Vict Descent").isNotNull()))

crime_incidents_2015_zip_df = crime_incidents_2015_df.join(rev_geocoding_unique_df, ['LAT', 'LON']) \
        .select(col("Vict Descent"), col("ZIPcode"))
# maybe use .persist()

distinct_zip_codes_df = crime_incidents_2015_zip_df.select("ZIPcode").distinct()


LA_income_formatted_df = LA_income_2015_df.select(col("Zip Code").alias("ZIPcode"), col("Estimated Median Income")) \
        .withColumn("Estimated Median Income",regexp_replace("Estimated Median Income", "[$,]", "").cast("integer"))


distinct_LA_incomes_df = LA_income_formatted_df.join(distinct_zip_codes_df, "ZIPcode") \
# maybe use .persist()

best_worst_3_df = distinct_LA_incomes_df.orderBy(col("Estimated Median Income")).limit(3) \
        .union(distinct_LA_incomes_df.orderBy(col("Estimated Median Income").desc()).limit(3))


query_3_df = crime_incidents_2015_zip_df.join(best_worst_3_df, "ZIPcode") \
        .withColumn(
                "Victim Descent", 
                when((col("Vict Descent") == "A") | (col("Vict Descent") == "C") | (col("Vict Descent") == "K") | (col("Vict Descent") == "J") | (col("Vict Descent") == "F"), "Asian") \
                .when((col("Vict Descent") == "B"), "Black") \
                .when((col("Vict Descent") == "W"), "White") \
                .when((col("Vict Descent") == "O") | (col("Vict Descent") == "I"), "Other") \
                .when((col("Vict Descent") == "H"), "Hispanic/Latin/Mexican") \
                .when((col("Vict Descent") == "X"), "Unknown")
                ) \
        .groupBy(col("Victim Descent")).count() \
        .orderBy(col("count").desc()) \
        .select(col("Victim Descent"), col("count").alias("#"))


query_3_df.show()

