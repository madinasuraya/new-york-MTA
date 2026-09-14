# congestion_delay_hotspots.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when

spark = SparkSession.builder.appName("CongestionDelayHotspots").getOrCreate()

INPUT = "s3://pdcbucket-aa2/raw/"
OUTPUT = "s3://pdcbucket-aa2/processed/congestion_delay_hotspots/"

# Threshold: lower ratio = worse performance (adjust if needed)
THRESHOLD = 0.75

df = spark.read.option("header", "true").csv(INPUT)

df = (df
      .withColumn("number_of_trips_passing_wait", col("number_of_trips_passing_wait").cast("double"))
      .withColumn("number_of_scheduled_trips", col("number_of_scheduled_trips").cast("double"))
)

agg = (df.groupBy("borough", "period")
         .agg(
             _sum("number_of_trips_passing_wait").alias("sum_trips_passing_wait"),
             _sum("number_of_scheduled_trips").alias("sum_scheduled_trips")
         )
         .withColumn(
             "efficiency_ratio",
             when(col("sum_scheduled_trips") > 0, col("sum_trips_passing_wait") / col("sum_scheduled_trips"))
             .otherwise(None)
         )
         .withColumn(
             "is_hotspot",
             when(col("efficiency_ratio").isNotNull() & (col("efficiency_ratio") < THRESHOLD), 1).otherwise(0)
         )
)

agg.write.mode("overwrite").option("header", "true").csv(OUTPUT)
spark.stop()
