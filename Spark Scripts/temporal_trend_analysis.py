# temporal_trend_analysis.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when
from pyspark.sql.window import Window
from pyspark.sql.functions import avg as _avg

spark = SparkSession.builder.appName("TemporalTrendAnalysis").getOrCreate()

INPUT = "s3://pdcbucket-aa2/raw/"
OUTPUT = "s3://pdcbucket-aa2/processed/temporal_trend_analysis/"

df = spark.read.option("header", "true").csv(INPUT)

df = (df
      .withColumn("month", col("month").cast("int"))
      .withColumn("number_of_trips_passing_wait", col("number_of_trips_passing_wait").cast("double"))
      .withColumn("number_of_scheduled_trips", col("number_of_scheduled_trips").cast("double"))
)

monthly = (df.groupBy("borough", "month")
             .agg(
                 _sum("number_of_trips_passing_wait").alias("sum_trips_passing_wait"),
                 _sum("number_of_scheduled_trips").alias("sum_scheduled_trips")
             )
             .withColumn(
                 "efficiency_ratio",
                 when(col("sum_scheduled_trips") > 0, col("sum_trips_passing_wait") / col("sum_scheduled_trips"))
                 .otherwise(None)
             )
)

# Rolling 3-month average within each borough
w = Window.partitionBy("borough").orderBy(col("month")).rowsBetween(-2, 0)
monthly = monthly.withColumn("rolling_3mo_efficiency", _avg("efficiency_ratio").over(w))

monthly.write.mode("overwrite").option("header", "true").csv(OUTPUT)
spark.stop()
