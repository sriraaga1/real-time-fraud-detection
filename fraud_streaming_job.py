from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StringType, DoubleType

# Step 1: Define the schema of incoming JSON data
schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("user_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("amount", DoubleType()) \
    .add("merchant", StringType()) \
    .add("city", StringType()) \
    .add("country", StringType()) \
    .add("device_type", StringType()) \
    .add("payment_method", StringType())

# Step 2: Create Spark session
spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 3: Read stream from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Step 4: Convert Kafka value from bytes to string
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

# Step 5: Parse JSON into columns
parsed_df = json_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

# Step 6: Add fraud rules
fraud_df = parsed_df.withColumn(
    "fraud_flag",
    when(col("amount") > 1500, "YES")
    .when(col("country") != "USA", "YES")
    .otherwise("NO")
)

fraud_df = fraud_df.withColumn(
    "fraud_reason",
    when(col("amount") > 1500, "high_amount")
    .when(col("country") != "USA", "non_usa_transaction")
    .otherwise("normal")
)

query = fraud_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/fraud_results") \
    .option("checkpointLocation", "output/checkpoints/fraud_results") \
    .start()

query.awaitTermination()