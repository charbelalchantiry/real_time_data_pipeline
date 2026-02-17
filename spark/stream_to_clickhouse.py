from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

spark = SparkSession.builder.appName("WeatherStreamingToClickHouse").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Kafka message schema (matches your producer payload) ---
schema = StructType([
    StructField("name", StringType(), True),
    StructField("sys", StructType([
        StructField("country", StringType(), True),
    ]), True),
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
    ]), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
    ]), True),
    StructField("weather", ArrayType(StructType([
        StructField("description", StringType(), True),
    ])), True),
])

# --- Read stream from Kafka ---
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_raw")
    .option("startingOffsets", "latest")
    .option("checkpointLocation", "/tmp/checkpoints/weather_stream")
    .load()
)

# Parse JSON and keep event time from Kafka (timestamp column)
json_df = kafka_df.select(
    col("timestamp").alias("event_time"),
    from_json(col("value").cast("string"), schema).alias("j")
).select("event_time", "j.*")

processed = json_df.select(
    col("event_time"),
    col("name").alias("city"),
    col("main.temp").alias("temperature_c")
).withWatermark("event_time", "2 minutes")


# --- Analytics 1: events per 10 seconds ---
events_per_window = (
    processed
    .groupBy(window(col("event_time"), "10 seconds"))
    .agg(count("*").alias("events"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("events")
    )
)

# --- Analytics 2: avg temp per city per 1 minute ---
avg_temp_by_city = (
    processed
    .groupBy(window(col("event_time"), "1 minute"), col("city"))
    .agg(avg(col("temperature_c")).alias("avg_temp_c"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("avg_temp_c")
    )
)

# --- ClickHouse JDBC settings (inside docker network) ---
jdbc_url = "jdbc:clickhouse://clickhouse:8123/weather"
jdbc_props = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "spark",
    "password": "sparkpass"
}

def write_events(batch_df, batch_id):
    (batch_df.write
        .mode("append")
        .jdbc(url=jdbc_url, table="events_per_window", properties=jdbc_props)
    )

def write_avg(batch_df, batch_id):
    (batch_df.write
        .mode("append")
        .jdbc(url=jdbc_url, table="avg_temp_by_city", properties=jdbc_props)
    )

q1 = (events_per_window.writeStream
      .outputMode("append")
      .foreachBatch(write_events)
      .option("checkpointLocation", "/tmp/chk/events_per_window")
      .start())

q2 = (avg_temp_by_city.writeStream
      .outputMode("append")
      .foreachBatch(write_avg)
      .option("checkpointLocation", "/tmp/chk/avg_temp_by_city")
      .start())

spark.streams.awaitAnyTermination()
