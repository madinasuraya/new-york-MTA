# borough_aggregation.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, when

spark = SparkSession.builder.appName("BoroughAggregation").getOrCreate()

INPUT = "s3://pdcbucket-aa2/raw/"
OUTPUT = "s3://pdcbucket-aa2/processed/borough_aggregation/"

df = spark.read.option("header", "true").csv(INPUT)

df = (df
      .withColumn("number_of_trips_passing_wait", col("number_of_trips_passing_wait").cast("double"))
      .withColumn("number_of_scheduled_trips", col("number_of_scheduled_trips").cast("double"))
)

result = (df.groupBy("borough")
            .agg(
                avg("number_of_trips_passing_wait").alias("avg_trips_passing_wait"),
                avg("number_of_scheduled_trips").alias("avg_scheduled_trips"),
                _sum("number_of_trips_passing_wait").alias("sum_trips_passing_wait"),
                _sum("number_of_scheduled_trips").alias("sum_scheduled_trips")
            )
            .withColumn(
                "efficiency_ratio",
                when(col("sum_scheduled_trips") > 0, col("sum_trips_passing_wait") / col("sum_scheduled_trips"))
                .otherwise(None)
            )
)

result.write.mode("overwrite").option("header", "true").csv(OUTPUT)
spark.stop()
