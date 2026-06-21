from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    LongType,
)

from config import EVENT_TYPE

def edge_raw_schema() -> StructType:
    return StructType(
        [
            StructField("src", StringType(), False),
            StructField("dst", StringType(), False),
            StructField("relationship", StringType(), False),
            StructField("timestamp", TimestampType(), True),
            StructField("price", DoubleType(), True),
            StructField("epoch_id", LongType(), False),
        ]
    )


def vertex_raw_schema() -> StructType:
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
            StructField("epoch_id", LongType(), False),
        ]
    )


def edges_schema() -> StructType:
    return StructType(
        [
            StructField("src", StringType(), False),
            StructField("dst", StringType(), False),
            StructField("score", DoubleType(), False),
            StructField("last_interaction", TimestampType(), True),
            *[StructField(et, LongType(), False) for et in EVENT_TYPE],
        ]
    )


def vertices_schema() -> StructType:
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("type", StringType(), True),
        ]
    )