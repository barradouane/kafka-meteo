"""Exercice 5 - Spark : agrégats en fenêtre glissante (1 min, pas de 30 s) sur weather_transformed."""
import os
import time
import shutil
import pandas as pd
from IPython.display import clear_output, display
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

# Nettoyage du checkpoint pour forcer la relecture propre
shutil.rmtree("/tmp/checkpoints/ex5_console", ignore_errors=True)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SOURCE_TOPIC = "weather_transformed"

spark = SparkSession.builder.appName("WeatherAggregates").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

SCHEMA_TRANSFORMED = StructType([
    StructField("event_time", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("weather_code", IntegerType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType()),
])

def parse_transformed(raw):
    return (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA_TRANSFORMED).alias("d"))
               .select("d.*")
               .where(F.col("event_time").isNotNull()))

def aggregate_windows(df):
    def flag(column, level):
        return F.sum(F.when(F.col(column) == level, 1).otherwise(0))

    wind_alert = F.col("wind_alert_level").isin("level_1", "level_2")
    heat_alert = F.col("heat_alert_level").isin("level_1", "level_2")

    agg = (
        # Watermark élargie à 7 jours pour accepter l'historique complet sans perte
        df.withWatermark("event_time", "7 days")
        .groupBy(F.window("event_time", "1 minute", "30 seconds"), "country", "city")
        .agg(
            flag("wind_alert_level", "level_1").alias("wind_level_1"),
            flag("wind_alert_level", "level_2").alias("wind_level_2"),
            flag("heat_alert_level", "level_1").alias("heat_level_1"),
            flag("heat_alert_level", "level_2").alias("heat_level_2"),
            F.sum(F.when(wind_alert, 1).otherwise(0) + F.when(heat_alert, 1).otherwise(0)).alias("total_alerts"),
            F.round(F.avg("temperature"), 2).alias("avg_temp"),
            F.min("temperature").alias("min_temp"),
            F.max("temperature").alias("max_temp"),
        )
    )
    return agg.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "*",
    ).drop("window")

# Lecture depuis Kafka et calcul
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df_agg = aggregate_windows(parse_transformed(raw_stream))

# Affichage console robuste
query = df_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoints/ex5_console") \
    .trigger(processingTime="5 seconds") \
    .start()

print("Exercice 5 lancé avec l'historique ")
try:
    while query.isActive:
        time.sleep(5)
except KeyboardInterrupt:
    query.stop()
    print("\nFlux arrêté.")