from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, collect_set, size
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("card_brand", StringType(), True),
    StructField("country", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

df_transactions = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# - withWatermark: It allows data to be delayed for up to 5 minutes on the network without breaking the logic.
# - window: Creates 2-minute time slices that re-evaluate every 10 seconds.
# - filter: If the same user_id made purchases in more than one different country within those two minutes -> Alert!
df_alerts = df_transactions \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window(col("timestamp"), "2 minutes", "10 seconds"),
        col("user_id")
    ) \
    .agg(collect_set("country").alias("countries_list")) \
    .filter(size(col("countries_list")) > 1)

query = df_alerts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("🚀 Pipeline de pé! Monitorando fraudes ativamente... Aguardando lotes de dados...")
query.awaitTermination()