"""
main.py — Point d'entrée applicatif.

Importe spark_streaming et reçoit les DataFrames vertices/edges
à chaque micro-batch pour les traiter dans handle_graph.
"""

from pyspark.sql import DataFrame

from spark_streaming import start_streams
from graph import handle_new_data
from config import get_spark_session
 

def handle_graph(vertices: DataFrame, edges: DataFrame, epoch_id: int) -> None:
    """
    Callback appelé à chaque micro-batch avec les DataFrames raw des edges et vertices
    (pas encore agrégés).

    Parameters
    ----------
    vertices : DataFrame  — colonnes : id (str), type (USER | PROD | SEL)
    edges    : DataFrame  — colonnes : src (str), dst (str), relationship (str),
                            price (float), timestamp (timestamp)
    epoch_id : int        — numéro du batch courant
    """
    handle_new_data(vertices, edges, epoch_id)


def main():
    spark = get_spark_session()

    q1, q2 = start_streams(
        spark=spark,
        kafka_bootstrap="localhost:9092",
        kafka_topic="marketplace-events",
        on_graph=handle_graph,  # ← injection du callback
        await_termination=False,  # ← on garde la main ici
    )

    print("Streams démarrés. En attente…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()