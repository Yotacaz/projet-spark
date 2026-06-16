"""
main.py — Point d'entrée applicatif.

Importe spark_streaming et reçoit les DataFrames vertices/edges
à chaque micro-batch pour les traiter dans handle_graph.
"""

from pyspark.sql import DataFrame
from graphframes import GraphFrame
from pyspark.sql.functions import col

from spark_streaming import build_spark_session, start_streams


def handle_graph(vertices: DataFrame, edges: DataFrame, epoch_id: int) -> None:
    # ici traite les donnees
    """
    Callback appelé à chaque micro-batch avec les DataFrames prêts.

    Parameters
    ----------
    vertices : DataFrame  — colonnes : id (str), type (USER | PROD | SEL)
    edges    : DataFrame  — colonnes : src (str), dst (str), relationship (str)
    epoch_id : int        — numéro du batch courant
    """
    print(f"\n=== [main.py] Batch {epoch_id} reçu ===")
    print(f"  Vertices : {vertices.count()} nœuds")
    print(f"  Edges    : {edges.count()} arêtes")

    # Ici traite les donnees.


def main():
    spark = build_spark_session()

    q1, q2 = start_streams(
        spark=spark,
        kafka_bootstrap="localhost:9092",
        kafka_topic="marketplace-events",
        on_graph=handle_graph,   # ← injection du callback
        await_termination=False, # ← on garde la main ici
    )

    print("Streams démarrés. En attente…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()