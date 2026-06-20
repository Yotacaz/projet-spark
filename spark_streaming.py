from config import get_spark_session

from typing import Callable, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, count, sum as spark_sum
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType,
)

# ── Schéma identique à ce que generer_evenement() produit ───────────────────
SCHEMA = StructType([
    StructField("timestamp",   StringType(), True),
    StructField("user_id",     StringType(), True),
    StructField("user_city",   StringType(), True),
    StructField("product_id",  StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id",   StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price",       DoubleType(), True),
])


def build_graph_dataframes(batch_df: DataFrame):
    """
    Construit et retourne (vertices, edges) depuis un micro-batch.

    Parameters
    ----------
    batch_df : DataFrame
        Micro-batch issu du stream Kafka désérialisé.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        (vertices, edges) prêts à l'emploi.
        edges colonnes : src, dst, relationship, timestamp, price
    """
    from pyspark.sql.functions import lit, when

    vertices = (
        batch_df.select(col("user_id").alias("id"),     lit("USER").alias("type"))
        .union(batch_df.select(col("product_id").alias("id"), lit("PROD").alias("type")))
        .union(batch_df.select(col("seller_id").alias("id"),  lit("SEL").alias("type")))
        .dropDuplicates(["id"])
    )

    edges = batch_df.select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        col("action_type").alias("relationship"),
        col("timestamp"),
        when(col("action_type") == "like", lit(0.0))
        .otherwise(col("price"))
        .alias("price"),
    )

    return vertices, edges


def make_batch_processor(
    on_graph: Optional[Callable[[DataFrame, DataFrame, int], None]] = None,
) -> Callable[[DataFrame, int], None]:
    from graphframes import GraphFrame
 
    def _default_on_graph(vertices: DataFrame, edges: DataFrame, epoch_id: int):
        g = GraphFrame(vertices, edges)
        print(f"\n--- [Batch {epoch_id}] Top 5 Produits les plus populaires ---")
        g.inDegrees.orderBy(col("inDegree").desc()).show(5)

    handler = on_graph if on_graph is not None else _default_on_graph

    def process_batch(batch_df: DataFrame, epoch_id: int):
        if batch_df.isEmpty():
            return
        vertices, edges = build_graph_dataframes(batch_df)
        handler(vertices, edges, epoch_id)

    return process_batch


def start_streams(
    spark: Optional[SparkSession] = None,
    kafka_bootstrap: str = "localhost:9092",
    kafka_topic: str = "marketplace-events",
    on_graph: Optional[Callable[[DataFrame, DataFrame, int], None]] = None,
    await_termination: bool = True,
    enable_console: bool = False,
):
    if spark is None:
        spark = get_spark_session()

    # ── Lecture Kafka ────────────────────────────────────────────────────────
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # ── Désérialisation JSON ─────────────────────────────────────────────────
    df_parsed = (
        df_raw
        .select(from_json(col("value").cast("string"), SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    )

    # ── Query principale : graphe via foreachBatch ───────────────────────────
    q2 = (
        df_parsed.writeStream
        .foreachBatch(make_batch_processor(on_graph))
        .trigger(processingTime="10 seconds")
        .start()
    )

    # ── Query console : agrégations (optionnelle) ────────────────────────────
    q1 = None
    if enable_console:
        df_wm = df_parsed.withWatermark("timestamp", "1 minute")
        agg = (
            df_wm
            .groupBy(window(col("timestamp"), "1 minute"), col("action_type"))
            .agg(
                count("*").alias("nb_events"),
                spark_sum("price").alias("chiffre_affaires"),
            )
        )
        q1 = (
            agg.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", "false")
            .trigger(processingTime="30 seconds")
            .start()
        )

    if await_termination:
        spark.streams.awaitAnyTermination()

    return q1, q2


# ── Point d'entrée standalone (comportement original) ───────────────────────
if __name__ == "__main__":
    start_streams()