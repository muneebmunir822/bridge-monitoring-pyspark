from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

def run_silver_enrichment():
    spark = SparkSession.builder \
        .appName("SilverEnrichment") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    bronze_temperature_path = "content/bronze/bridge_temperature"
    bronze_vibration_path = "content/bronze/bridge_vibration"
    bronze_tilt_path = "content/bronze/bridge_tilt"

    temperature_df = spark.read.parquet(bronze_temperature_path)
    vibration_df = spark.read.parquet(bronze_vibration_path)
    tilt_df = spark.read.parquet(bronze_tilt_path)

    metadata_path = "bridge-monitoring-pyspark/metadata/bridges.csv"
    bridges_df = spark.read.option("header", True).csv(metadata_path)

    temperature_enriched = temperature_df.join(bridges_df, temperature_df.bridge_id == bridges_df.id, "left") \
                                         .withColumn("enriched_date", current_date())

    vibration_enriched = vibration_df.join(bridges_df, vibration_df.bridge_id == bridges_df.id, "left") \
                                     .withColumn("enriched_date", current_date())

    tilt_enriched = tilt_df.join(bridges_df, tilt_df.bridge_id == bridges_df.id, "left") \
                           .withColumn("enriched_date", current_date())

    temperature_enriched.write.mode("overwrite").parquet("bridge-monitoring-pyspark/silver/bridge_temperature")
    vibration_enriched.write.mode("overwrite").parquet("bridge-monitoring-pyspark/silver/bridge_vibration")
    tilt_enriched.write.mode("overwrite").parquet("bridge-monitoring-pyspark/silver/bridge_tilt")

    print("Silver enrichment completed.")

    spark.stop()

if __name__ == "__main__":
    run_silver_enrichment()
