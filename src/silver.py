from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
import os


base_dir = os.path.dirname(os.path.dirname(__file__))

input_path = os.path.join(base_dir, "Data", "raw", "taxi_trips.csv")
output_path = os.path.join(base_dir, "Data", "Silver", "taxi_trips_silver.parquet")


spark_input_path = "file:///" + input_path.replace("\\", "/")
spark_output_path = "file:///" + output_path.replace("\\", "/")

# Start Spark session
spark = SparkSession.builder \
    .appName("Taxi Silver Layer") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print("Spark is working")

# Load Bronze data (csv)
df = spark.read.option("header", True).option("inferSchema", True).csv(spark_input_path)

# Remove duplicates
df = df.dropDuplicates()

# Remove invalid trips
df = df.filter(col("trip_distance") > 0)
df = df.filter(col("fare_amount") > 0)

# Convert datetime
df = df.withColumn("pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
df = df.withColumn("dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))

# Create trip duration (minutes)
df = df.withColumn(
    "trip_duration_min",
    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
)

# Remove outliers
df = df.filter(col("trip_duration_min") > 0)
df = df.filter(col("trip_duration_min") < 300)

# ---------------------------
# Save Silver
# ---------------------------
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df.write.mode("overwrite").parquet(spark_output_path)

print("Silver layer loaded successfully!")