import os
import sys

#----! Adapter ceci pour votre machine ou retirer le !-----#
# Force Java 17

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if sys.platform == "win32":
    os.environ["JAVA_HOME"]  = "C:/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
    os.environ["HADOOP_HOME"] = "C:/hadoop"
    os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
    os.environ["JAVA_TOOL_OPTIONS"] = (
        os.environ.get("JAVA_TOOL_OPTIONS", "") +
        " -Djavax.security.auth.useSubjectCredsOnly=false"
    )
#-----------------------------------------------------------#

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as spark_sum
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("MarketplaceKafka") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ── Schéma identique à ce que generer_evenement() produit ───────────────────
schema = StructType([
    StructField("timestamp",   StringType(), True),
    StructField("user_id",     StringType(), True),
    StructField("user_city",   StringType(), True),
    StructField("product_id",  StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id",   StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price",       DoubleType(), True),
])

# ── Lecture Kafka ────────────────────────────────────────────────────────────
# Avec Kafka, Spark reçoit les colonnes : key, value, topic, partition,
# offset, timestamp, timestampType. Seule "value" contient le JSON.
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "marketplace-events") \
    .option("startingOffsets", "latest") \
    .load()

# ── Désérialisation JSON ─────────────────────────────────────────────────────
df_parsed = df_raw \
    .select(from_json(col("value").cast("string"), schema).alias("d")) \
    .select("d.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# ── Watermark + fenêtrage ────────────────────────────────────────────────────
df_wm = df_parsed.withWatermark("timestamp", "10 minutes")

agg = df_wm \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("action_type")
    ) \
    .agg(
        count("*").alias("nb_events"),
        spark_sum("price").alias("chiffre_affaires")
    )

# ── foreachBatch → GraphFrames ───────────────────────────────────────────────
from graphframes import GraphFrame

def process_batch(batch_df, epoch_id):
    if batch_df.isEmpty():
        return

    from pyspark.sql.functions import lit

    vertices = batch_df.select(col("user_id").alias("id"), lit("USER").alias("type")) \
        .union(batch_df.select(col("product_id").alias("id"), lit("PROD").alias("type"))) \
        .union(batch_df.select(col("seller_id").alias("id"),  lit("SEL").alias("type"))) \
        .dropDuplicates(["id"])

    edges = batch_df.select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        col("action_type").alias("relationship")
    )

    g = GraphFrame(vertices, edges)
    g.inDegrees.show(5)

# ── Lancement des deux queries en parallèle ──────────────────────────────────
q1 = agg.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

q2 = df_wm.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()