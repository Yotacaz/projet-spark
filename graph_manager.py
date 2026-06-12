from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware



spark = (
    SparkSession.builder.appName("MarketplaceGraph")
    .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.4-s_2.12")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setCheckpointDir("./checkpoints")

from graphframes import GraphFrame  # noqa: E402

NODE_CAT= ["USER", "PRODUCT", "SELLER"]
EDGE_CAT= ["LIKE","SELL","VOUTCH"]



def build_vertices(edges_df: DataFrame) -> DataFrame:

    vertices = (
        edges_df.select(col("src").alias("id"))
        .union(edges_df.select(col("dst").alias("id")))
        .distinct()
        .withColumn(
            "type",
            when(col("id").startswith("U"), "USER")
            .when(col("id").startswith("P"), "PRODUCT")
            .when(col("id").startswith("S"), "SELLER")
            .otherwise("UNKNOWN"),
        )
        .withColumn("label", col("id"))
    )

    return vertices


def build_graph(edges_df: DataFrame) -> GraphFrame:
    vertices = build_vertices(edges_df)
    return GraphFrame(vertices, edges_df)


def show_basic_metrics(graph: GraphFrame):

    print("\n===== VERTICES =====")
    print(graph.vertices.count())
    print("\n===== EDGES =====")
    print(graph.edges.count())
    print("\n===== DEGREES =====")
    graph.degrees.orderBy(col("degree").desc()).show(20, truncate=False)
    print("\n===== IN DEGREES =====")
    graph.inDegrees.orderBy(col("inDegree").desc()).show(20, truncate=False)
    print("\n===== OUT DEGREES =====")
    graph.outDegrees.orderBy(col("outDegree").desc()).show(20, truncate=False)



def compute_connected_components(graph: GraphFrame):
    print("\n===== CONNECTED COMPONENTS =====")
    graph.connectedComponents().groupBy("component").count().orderBy(
        col("count").desc()
    ).show(truncate=False)


def get_neighborhood(graph: GraphFrame, node_id: str):
    edges = graph.edges.filter(
        (col("src") == node_id) |
        (col("dst") == node_id)
    )

    vertices = (
        edges.select(col("src").alias("id"))
        .union(edges.select(col("dst").alias("id")))
        .distinct()
        .join(graph.vertices, "id")
    )

    return vertices, edges

# app = FastAPI()
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# @app.get("/graph/node/{node_id}")
# def get_graph(node_id: str):
#     vertices, edges = get_neighborhood(graph, node_id)

#     return {
#         "nodes": vertices.toPandas().to_dict("records"),
#         "edges": edges.toPandas().to_dict("records"),
#     }

def create_test_dataset() -> DataFrame:
    """"""
    return spark.createDataFrame(
        [
            ("U1", "P1", "LIKE", 1.0),
            ("U2", "P1", "LIKE", 20.0),
            ("U3", "P2", "LIKE", 5.0),
            ("U1", "P2", "LIKE", 1.0),
            ("S1", "P1", "SELL", 1.0),
            ("S2", "P2", "SELL", 1.0),
        ],
        ["src", "dst", "relationship", "weight"],
    )

if __name__ == "__main__":
    edges_df = create_test_dataset()
    print("\n===== INPUT =====")
    edges_df.show(truncate=False)
    graph = build_graph(edges_df)
    show_basic_metrics(graph)
    # compute_pagerank(graph)
    compute_connected_components(graph)
    spark.stop()


