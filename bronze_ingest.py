from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

RAW_SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("bridge_id", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("ingest_time", StringType(), True)
])

spark = SparkSession.builder.appName("bronze-ingest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def create_stream(path):
    return spark.readStream.schema(RAW_SCHEMA) \
        .option("header", True) \
        .option("maxFilesPerTrigger", 1) \
        .csv(path)

temperature_stream = create_stream("bridge-monitoring-pyspark/streams/bridge_temperature/")
vibration_stream = create_stream("bridge-monitoring-pyspark/streams/bridge_vibration/")
tilt_stream = create_stream("bridge-monitoring-pyspark/streams/bridge_tilt/")

queries = [
    temperature_stream.writeStream.format("console").option("truncate", False).start(),
    vibration_stream.writeStream.format("console").option("truncate", False).start(),
    tilt_stream.writeStream.format("console").option("truncate", False).start()
]

for query in queries:
    query.awaitTermination()
