from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

def run_gold_aggregation():
    spark = SparkSession.builder \
        .appName("GoldAggregation") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    silver_temp_path = "bridge-monitoring-pyspark/silver/bridge_temperature"
    silver_vib_path = "bridge-monitoring-pyspark/silver/bridge_vibration"
    silver_tilt_path = "bridge-monitoring-pyspark/silver/bridge_tilt"

    temp_df = spark.read.parquet(silver_temp_path)
    vib_df = spark.read.parquet(silver_vib_path)
    tilt_df = spark.read.parquet(silver_tilt_path)

    temp_agg = temp_df.groupBy("bridge_id", "partition_date") \
                      .agg(avg("value").alias("avg_temperature"))

    vib_agg = vib_df.groupBy("bridge_id", "partition_date") \
                    .agg(avg("value").alias("avg_vibration"))

    tilt_agg = tilt_df.groupBy("bridge_id", "partition_date") \
                      .agg(avg("value").alias("avg_tilt"))

    joined_df = temp_agg.join(vib_agg, ["bridge_id", "partition_date"], "outer") \
                        .join(tilt_agg, ["bridge_id", "partition_date"], "outer") \
                        .orderBy("bridge_id", "partition_date")

    joined_df.write.mode("overwrite").parquet("/content/bridge-monitoring-pyspark/gold/aggregated_metrics")

    print("Gold aggregation completed.")
    spark.stop()

if __name__ == "__main__":
    run_gold_aggregation()
