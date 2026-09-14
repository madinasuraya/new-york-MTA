# bus_asessment_standardized.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace, to_date, when, lit
)

spark = SparkSession.builder.appName("BusAsessmentStandardized").getOrCreate()

INPUT  = "s3://pdcbucket-aa2/raw/"
OUTPUT = "s3://pdcbucket-aa2/standardized/bus_wait_assessment/" 

df = spark.read.option("header", "true").csv(INPUT)

df = (
    df
    # trim spaces in key string fields
    .withColumn("borough", trim(col("borough")))
    .withColumn("route_id", trim(col("route_id")))
    .withColumn("period", trim(col("period")))
    .withColumn("day_type", trim(col("day_type")))
    .withColumn("trip_type", trim(col("trip_type")))

    # standardize text casing for consistency
    .withColumn("borough", regexp_replace(upper(col("borough")), r"\s+", " "))
    .withColumn("period", regexp_replace(col("period"), r"\s+", " "))
    .withColumn("day_type", regexp_replace(col("day_type"), r"\s+", " "))
    .withColumn("trip_type", regexp_replace(col("trip_type"), r"\s+", " "))

    # cast numeric columns safely
    .withColumn("number_of_scheduled_trips", col("number_of_scheduled_trips").cast("double"))
    .withColumn("number_of_trips_passing_wait", col("number_of_trips_passing_wait").cast("double"))
    .withColumn("wait_assessment", col("wait_assessment").cast("double"))
)

df = df.filter(
    col("borough").isNotNull() &
    col("route_id").isNotNull() &
    col("period").isNotNull() &
    col("number_of_scheduled_trips").isNotNull()
)

df = df.filter(
    (col("number_of_scheduled_trips") >= 0) &
    (col("number_of_trips_passing_wait") >= 0)
)

df = df.withColumn(
    "month",
    to_date(col("month"), "M/d/yyyy")
)

df.write.mode("overwrite").option("header", "true").csv(OUTPUT)

spark.stop()
