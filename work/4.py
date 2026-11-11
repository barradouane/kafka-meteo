import time
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

def get_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_kafka(spark, topic):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load())

SOURCE_TOPIC = "weather_stream"
TARGET_TOPIC = "weather_transformed"


SCHEMA_RAW = StructType([
    StructField("current", StructType([
        StructField("time", StringType()),
        StructField("interval", IntegerType()),
        StructField("temperature_2m", DoubleType()),
        StructField("wind_speed_10m", DoubleType()),
        StructField("weather_code", IntegerType()),
    ])),
    StructField("city", StringType()),
    StructField("country", StringType()),
])

OUTPUT_COLUMNS = ["event_time", "temperature", "windspeed", "city", "country",
                  "weather_code", "wind_alert_level", "heat_alert_level"]

def transform_weather(raw):
    parsed = (raw.select(
        F.from_json(F.col("value").cast("string"), SCHEMA_RAW).alias("d")
    ).withColumn(
        "event_time", F.to_timestamp(F.col("d.current.time"))
    ))
    flat = parsed.select(
        "event_time",
        F.col("d.current.temperature_2m").alias("temperature"),
        F.col("d.current.wind_speed_10m").alias("windspeed"),
        F.col("d.city").alias("city"),
        F.col("d.country").alias("country"),
        F.col("d.current.weather_code").alias("weather_code"),
    ).where(
        F.col("city").isNotNull() & F.col("temperature").isNotNull() & F.col("windspeed").isNotNull()
    )
    return (
        flat
        .withColumn("wind_alert_level",
                    F.when(F.col("windspeed") < 10, "level_0")
                     .when(F.col("windspeed") <= 20, "level_1")
                     .otherwise("level_2"))
        .withColumn("heat_alert_level",
                    F.when(F.col("temperature") < 25, "level_0")
                     .when(F.col("temperature") <= 35, "level_1")
                     .otherwise("level_2"))
        .select(*OUTPUT_COLUMNS)
    )
def start_transform(spark):
    out = transform_weather(read_kafka(spark, SOURCE_TOPIC))
    payload = out.select(
        F.col("city").alias("key"),
        F.to_json(F.struct(*OUTPUT_COLUMNS)).alias("value"),
    )
    return (
        payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", TARGET_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoints/ex4_transform")
        .trigger(processingTime="10 seconds")
        .queryName("ex4_transform")
        .start()
    )

if __name__ == "__main__":
    spark = get_spark("WeatherTransform")
    query = start_transform(spark)
    print("Exercice 4 lancé : weather_stream -> weather_transformed")
    try:
        while query.isActive:
            time.sleep(5)
    except KeyboardInterrupt:
        query.stop()
        print("\nFlux arrêté proprement.")