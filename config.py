from typing import cast
import os
import platform
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if platform.system() == "Windows":
    os.environ["JAVA_HOME"]  = "C:/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
    os.environ["HADOOP_HOME"] = "C:/hadoop"
    os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
    os.environ["JAVA_TOOL_OPTIONS"] = (
        os.environ.get("JAVA_TOOL_OPTIONS", "") +
        " -Djavax.security.auth.useSubjectCredsOnly=false"
    )
elif platform.system() == "Linux":
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ.get("PATH", "")
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from pyspark.sql import SparkSession

def get_spark_session() -> SparkSession:
    """Crée et retourne une SparkSession configurée."""
    builder: SparkSession.Builder = cast(
        SparkSession.Builder, SparkSession.builder
    )  # for missing type hints
    spark = (
        builder.appName("MarketplaceGraph")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                    "io.delta:delta-spark_2.12:3.2.0",
                    # GraphFrames — requis par le cahier des charges §3.1 pour le
                    # calcul d'indicateurs de centralité (pageRank) et de
                    # composants connectés (connectedComponents).
                    "graphframes:graphframes:0.8.3-spark3.0-s_2.12",
                ]
            ),
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    if spark is None:
        raise RuntimeError("Impossible de créer la SparkSession.")
    # connectedComponents() de GraphFrames a besoin d'un checkpoint directory
    # (il s'appuie sur l'algorithme GraphX qui checkpointe pour éviter un DAG
    # de lignage trop profond). Sans ça, l'appel lève une exception au runtime.
    spark.sparkContext.setCheckpointDir("checkpoints/graphframes")
    return spark


RELATIONSHIP_SCORES = {
    "AIME": 1.0,
    "VOUT": 5.0,
    "ACHAT": 20.0,
}
EVENT_TYPE: list[str] = list(RELATIONSHIP_SCORES.keys())

DELTA_PATH = "delta/my_table"

VERTICES_PATH = f"{DELTA_PATH}/vertices"
EDGES_PATH = f"{DELTA_PATH}/edges"
TOP_EDGE_PATH = f"{DELTA_PATH}/top_edges"
EDGES_RAW_PATH = f"{DELTA_PATH}/edges_raw"
VERTICES_RAW_PATH = f"{DELTA_PATH}/vertices_raw"

if __name__ == "__main__":
    spark = get_spark_session()
    print("SparkSession créée avec succès.")