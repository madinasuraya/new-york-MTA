# route_efficiency_scoring.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, desc

spark = SparkSession.builder.appName("RouteEfficiencyScoring").getOrCreate()

INPUT = "s3://pdcbucket-aa2/raw/"
OUTPUT = "s3://pdcbucket-aa2/processed/route_efficiency_scoring/"

df = spark.read.option("header", "true").csv(INPUT)

df = (df
      .withColumn("number_of_trips_passing_wait", col("number_of_trips_passing_wait").cast("double"))
      .withColumn("number_of_scheduled_trips", col("number_of_scheduled_trips").cast("double"))
)

route = (df.groupBy("route_id")
           .agg(
               _sum("number_of_trips_passing_wait").alias("sum_trips_passing_wait"),
               _sum("number_of_scheduled_trips").alias("sum_scheduled_trips")
           )
           .withColumn(
               "efficiency_score",
               when(col("sum_scheduled_trips") > 0, col("sum_trips_passing_wait") / col("sum_scheduled_trips"))
               .otherwise(None)
           )
           .orderBy(desc("efficiency_score"))
)

route.write.mode("overwrite").option("header", "true").csv(OUTPUT)
spark.stop()
