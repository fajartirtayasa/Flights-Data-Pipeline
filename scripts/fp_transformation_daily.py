from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import col, from_unixtime, expr

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Remote Hive Metastore Connection") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Set current time variabel
current_date = datetime.today()

# Calculate the date 2 days before the current date
begin_date = current_date - timedelta(days=2)
begin_timestamp = int(begin_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
begin_date_string = begin_date.strftime("%d_%B_%Y")

# Calculate the date 1 day before the current date
end_date = current_date - timedelta(days=1)
end_timestamp = int(end_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

df1 = spark.sql(f"select * from flights_staging.fact_flight_dep where firstSeen between {begin_timestamp} and {end_timestamp}")
df1 = df1.dropDuplicates()
df2 = spark.sql(f"select * from flights_staging.fact_flight_arr where lastSeen between {begin_timestamp} and {end_timestamp}")
df2 = df2.dropDuplicates()

# Union 2 DataFrame
df = df1.union(df2)

# Add column "flight_duration_temp" to df_fact1
df = df.withColumn("flight_duration", expr("lastSeen - firstSeen"))

# Convert column firstSeen & lastSeen into datetime format
df = df.withColumn("firstSeen", from_unixtime(col("firstSeen")).cast("timestamp"))
df = df.withColumn("lastSeen", from_unixtime(col("lastSeen")).cast("timestamp"))

# Add surrogate key column derived from departure_date and arrival_date
df = df.withColumn("sk_departure_date", expr("date_format(firstSeen, 'yyyyMMdd')").cast("bigint"))
df = df.withColumn("sk_arrival_date", expr("date_format(lastSeen, 'yyyyMMdd')").cast("bigint"))

# Add surrogate key column derived from departure_time and arrival_time
df = df.withColumn("sk_departure_time", expr("hour(firstSeen)").cast("bigint"))
df = df.withColumn("sk_arrival_time", expr("hour(lastSeen)").cast("bigint"))

# List of new and old column names
rename_columns = [("firstSeen", "departure_time"), ("estDepartureAirport", "departure_airport"), ("lastSeen", "arrival_time"), ("estArrivalAirport", "arrival_airport")]

# Rename column using for loop
for old_col, new_col in rename_columns:
    df = df.withColumnRenamed(old_col, new_col)

# List of selected columns
selected_columns = ["ICAO24", "sk_departure_date", "sk_departure_time", "departure_airport", "sk_arrival_date", "sk_arrival_time", "arrival_airport", "callsign", "flight_duration"]

df_fact = df.select(selected_columns)
df_fact.show(5)
print('Total new data:', df.count())

df_fact.write.insertInto("flights_dwh.fact_flight", overwrite=False)