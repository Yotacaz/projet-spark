from pyspark.sql import DataFrame

from spark_streaming import start_streams
from graph.graph import handle_new_data, initialize_graph_storage
from config import get_spark_session


def handle_graph(vertices: DataFrame, edges: DataFrame, epoch_id: int) -> None:
    handle_new_data(vertices, edges, epoch_id)


def main():
    spark = get_spark_session()

    initialize_graph_storage(load_from_checkpoint=True)

    q1, q2 = start_streams(
        spark=spark,
        kafka_bootstrap="localhost:9092",
        kafka_topic="marketplace-events",
        on_graph=handle_graph,
        await_termination=False,
        enable_console=True,
    )

    print("Streams démarrés (ingestion). En attente…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()