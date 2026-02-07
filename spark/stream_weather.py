from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

spark = SparkSession.builder.appName("WeatherStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_raw") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("j")).select("j.*")

processed = json_df.select(
    col("name").alias("city"),
    col("sys.country").alias("country"),
    col("main.temp").alias("temperature_c"),
    col("main.humidity").alias("humidity"),
    col("wind.speed").alias("wind_speed"),
    col("weather")[0]["description"].alias("weather_description")
)

query = processed.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://weather-processed/stream_parquet/") \
    .option("checkpointLocation", "/tmp/checkpoints/weather_stream") \
    .start()

query.awaitTermination()
