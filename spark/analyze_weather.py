from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("WeatherAnalytics") \
    .getOrCreate()

# Read JSON files from MinIO (processed bucket)
df = spark.read.parquet("s3a://weather-processed/stream_parquet/")

df.select("city", "temperature_c", "humidity", "wind_speed").show(20, truncate=False)

# Simple analytics: average temperature per city
result = df.groupBy("city").agg(avg(col("temperature_c")).alias("avg_temp_c"))
result.show(truncate=False)

# Save analytics result to MinIO as parquet
result.write.mode("overwrite").parquet("s3a://weather-analytics/avg_temp_by_city/")

spark.stop()
 