from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType

spark = SparkSession.builder.appName("DroneTelemetry").getOrCreate()

schema = StructType([
    StructField("timestamp", DoubleType()),
    StructField("height", DoubleType()),
    StructField("pitch", DoubleType()),
    StructField("roll", DoubleType()),
    StructField("yaw", DoubleType()),
    StructField("battery", DoubleType())
])

raw_df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "drone_telemetry")
          .load())

telemetry_df = (raw_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*"))

# Convert numeric epoch seconds -> proper TIMESTAMP for windowing
telemetry_with_ts = (
    telemetry_df
        .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
        # allow 30 seconds of lateness; > window size (10s) is typical
        .withWatermark("event_time", "30 seconds")
)

agg_df = (telemetry_with_ts
          .groupBy(window(col("event_time"), "10 seconds"))
          .agg(
              avg("height").alias("avg_height"),
              avg("battery").alias("avg_battery"),
              avg("pitch").alias("avg_pitch"),
              avg("roll").alias("avg_roll"),
              avg("yaw").alias("avg_yaw")
          )
          .select(
              col("window.start").alias("start_time"),
              col("window.end").alias("end_time"),
              "avg_height",
              "avg_battery",
              "avg_pitch",
              "avg_roll",
              "avg_yaw"
          ))

query = (agg_df.writeStream
         .outputMode("append")
         .format("csv")
         .option("path", "aggregated_telemetry")
         .option("checkpointLocation", "checkpoint_dir")
         .option("header", "true")
         .start())

query.awaitTermination()