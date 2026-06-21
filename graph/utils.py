from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
)
import pyspark.sql.functions as F
from typing import Literal

from config import get_spark_session
from delta.tables import DeltaTable

spark = get_spark_session()

def _is_empty(df: DataFrame) -> bool:
    return len(df.take(1)) == 0


def _empty_like(df: DataFrame) -> DataFrame:
    return df.limit(0)


def _ensure_delta_table(path: str, schema: StructType) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], schema=schema).write.format("delta").save(path)

def _append_raw_delta(df: DataFrame, path: str, epoch_id: int, kind: Literal["edge", "vertex"]) -> None:
    if kind == "edge":
        prepared = df.withColumn("epoch_id", F.lit(epoch_id).cast("long"))
        if "price" not in prepared.columns:
            prepared = prepared.withColumn("price", F.lit(None).cast("double"))

        prepared = prepared.select(
            "src",
            "dst",
            "relationship",
            "timestamp",
            "price",
            "epoch_id",
        )

    else:
        prepared = (
            df.withColumn("epoch_id", F.lit(epoch_id).cast("long"))
            .select("id", "type", "epoch_id")
        )

    if not _is_empty(prepared):
        prepared.write.format("delta").mode("append").save(path)