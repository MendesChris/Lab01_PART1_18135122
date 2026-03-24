from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, to_date
import os
import psycopg2
import io


base_dir = os.path.dirname(os.path.dirname(__file__))
input_path = os.path.join(base_dir, "Data", "Silver", "taxi_trips_silver.parquet")


# Start Spark session
spark = SparkSession.builder \
    .appName("Taxi Silver Layer") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


# Load Silver Data
df = spark.read.parquet(input_path)

# DIM DATE
dim_date = df.select(
    to_date(col("pickup_datetime")).alias("date")
).dropDuplicates()

dim_date = dim_date.withColumn("year", year(col("date"))) \
                   .withColumn("month", month(col("date"))) \
                   .withColumn("day", dayofmonth(col("date"))) \
                   .withColumn("weekday", dayofweek(col("date")))

# FACT TABLE
fact = df.select(
    col("pickup_datetime"),
    col("dropoff_datetime"),
    col("passenger_count"),
    col("trip_distance"),
    col("fare_amount"),
    col("tip_amount"),
    col("total_amount"),
    col("payment_type"),
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id")
)

# PostgreSQL Connection
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="12571",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Create Tables
print("Creating tables...")

cur.execute("""
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date DATE,
    year INT,
    month INT,
    day INT,
    weekday INT
);
""")

cur.execute("""
DROP TABLE IF EXISTS fact_trips;
CREATE TABLE fact_trips (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT,
    payment_type INT,
    pickup_location_id INT,
    dropoff_location_id INT
);
""")

conn.commit()

# Function to stream Spark → PostgreSQL
def copy_from_spark(df, table_name):
    buffer = io.StringIO()

    for row in df.toLocalIterator():
        line = ",".join(
            "" if v is None else str(v)
            for v in row
        )
        buffer.write(line + "\n")

        # Flush in chunks (avoid memory issues)
        if buffer.tell() > 5_000_000:  # ~5MB
            buffer.seek(0)
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
            buffer.seek(0)
            buffer.truncate(0)

    # Final flush
    buffer.seek(0)
    cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
    buffer.close()

# Load Data
print("Loading dim_date...")
copy_from_spark(dim_date, "dim_date")
conn.commit()

print("Loading fact_trips...")
copy_from_spark(fact, "fact_trips")
conn.commit()

# Close
cur.close()
conn.close()

print("Gold layer loaded successfully")