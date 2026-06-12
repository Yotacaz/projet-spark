import os
import sys

#----! Adapter ceci pour votre machine ou retirer le !-----#
# Force Java 17
os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"

os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["JAVA_TOOL_OPTIONS"] = (
    os.environ.get("JAVA_TOOL_OPTIONS", "") +
    " -Djavax.security.auth.useSubjectCredsOnly=false"
)
#-----------------------------------------------------------#

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MarketplaceStreaming") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, window,
    count, sum as spark_sum, avg,
    to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

spark.sparkContext.setLogLevel("WARN")


# ── Schéma ───────────────────
#
#  Le simulateur produit ces 8 champs (voir ligne 270-278 de simulateur.py).
#  timestamp est un String ISO 8601 ("2026-05-25T09:15:30Z") → on le déclare
#  StringType ici et on le caste ensuite, car le format "Z" final n'est pas
#  parsé automatiquement par TimestampType en lecture directe.
#
schema = StructType([
    StructField("timestamp",   StringType(),  nullable=True),
    StructField("user_id",     StringType(),  nullable=True),
    StructField("user_city",   StringType(),  nullable=True),
    StructField("product_id",  StringType(),  nullable=True),
    StructField("product_cat", StringType(),  nullable=True),
    StructField("seller_id",   StringType(),  nullable=True),
    StructField("action_type", StringType(),  nullable=True),
    StructField("price",       DoubleType(),  nullable=True),
])


# ── Lecture du dossier produit par le simulateur ──────────────────────────
#
#  Spark surveille le dossier en continu. Dès qu'un nouveau fichier apparaît
#  (ou que evenements.json grossit), il est lu et traité.
#  maxFilesPerTrigger limite le nombre de fichiers par micro-batch
#  pour éviter de surcharger le premier trigger.
#
DOSSIER_SIMULATEUR = "./logs_simulateur/"   # ← ICI --output-dir

df_raw = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("multiLine", "false")  \
    .load(DOSSIER_SIMULATEUR)


# ── Cast du timestamp ──────────────────────────────────────────────────────
#
#  to_timestamp() avec le format explicite gère le "Z" final du simulateur
#  (ligne 271 : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")).
#
df_parsed = df_raw.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
)


# ── Watermark ──────────────────────────────────────────────────────────────
#
#  !! Le simulateur ne génère pas de données en retard !! (les timestamps sont
#  tous "now"). On met 10 minutes pour absorber :
#    - les délais de rotation/lecture de fichiers
#    - d'éventuels redémarrages du simulateur
#    - les micro-batches qui traitent des fichiers anciens au démarrage
#
df_wm = df_parsed \
    .withWatermark("timestamp", "10 minutes")


# ── Agrégations ────────────────────────────────────────────────────────────

# Volume d'actions par type, fenêtre de 5 minutes (tumbling)
agg_actions = df_wm \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("action_type")
    ) \
    .agg(
        count("*").alias("nb_events"),
        spark_sum("price").alias("chiffre_affaires"),
        avg("price").alias("prix_moyen")
    ) \
    .select(
        col("window.start").alias("debut"),
        col("window.end").alias("fin"),
        col("action_type"),
        col("nb_events"),
        col("chiffre_affaires"),
        col("prix_moyen")
    )

# Activité par ville, fenêtre glissante 10 min / pas 2 min
agg_villes = df_wm \
    .groupBy(
        window(col("timestamp"), "10 minutes", "2 minutes"),
        col("user_city")
    ) \
    .agg(count("*").alias("nb_actions")) \
    .select(
        col("window.start").alias("debut"),
        col("user_city"),
        col("nb_actions")
    )

# DataFrame pour GraphFrames — brut, sans agrégation
#     (passé via foreachBatch à GraphFrame)
df_pour_graphe = df_wm.select(
    "timestamp", "user_id", "product_id",
    "seller_id", "action_type", "price"
)


# ── Lancement des queries ──────────────────────────────────────────────────

q1 = agg_actions.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

q2 = agg_villes.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()