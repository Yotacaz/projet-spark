from __future__ import annotations

from typing import Optional, Literal, Final
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from graph.graph_store import GraphStore
from graph.schema import (
    edge_raw_schema,
    vertex_raw_schema,
    edges_schema,
    vertices_schema,
)
from graph.timer import timed
from config import (
    get_spark_session,
    EVENT_TYPE,
    VERTICES_PATH,
    EDGES_PATH,
    EDGES_RAW_PATH,
    VERTICES_RAW_PATH,
)
from graph.utils import _ensure_delta_table, _is_empty, _empty_like

spark = get_spark_session()

GRAPH_CHECKPOINT_DIR: Final[str] = "/tmp/graph_state_checkpoint"

GRAPH = GraphStore(GRAPH_CHECKPOINT_DIR)


def initialize_graph_storage(load_from_checkpoint: bool = True) -> None:
    """
    À appeler au démarrage du process.
    Crée les tables Delta si besoin puis recharge l'état si demandé.
    """
    _ensure_delta_table(EDGES_RAW_PATH, edge_raw_schema())
    _ensure_delta_table(VERTICES_RAW_PATH, vertex_raw_schema())
    _ensure_delta_table(EDGES_PATH, edges_schema())
    _ensure_delta_table(VERTICES_PATH, vertices_schema())

    if load_from_checkpoint:
        GRAPH.load_checkpoint()
    else:
        GRAPH.refresh_dataframes()


def invalidate_graph_cache() -> None:
    GRAPH.invalidate_cache()


def get_edges_and_vertices(
    force_reload: bool = False,
    persist: bool = False,
) -> tuple[DataFrame, DataFrame]:
    if force_reload:
        GRAPH.load_checkpoint()
    return GRAPH.get_dataframes(persist=persist)


@timed
def refresh_edges_and_vertices() -> tuple[DataFrame, DataFrame]:
    GRAPH.load_checkpoint()
    return GRAPH.get_dataframes(persist=False)


@timed
def handle_new_data(
    new_vertices_df: DataFrame,
    new_edges_df: DataFrame,
    epoch_id: int,
    checkpoint_every_n_batches: int = 20,
) -> None:
    GRAPH.apply_batch(
        new_vertices_df=new_vertices_df,
        new_edges_df=new_edges_df,
        epoch_id=epoch_id,
        checkpoint_every_n_batches=checkpoint_every_n_batches,
    )


def _incident_edges(
    edges_df: DataFrame,
    frontier_df: DataFrame,
    direction: Literal["both", "out", "in"],
) -> DataFrame:
    frontier = F.broadcast(frontier_df.select("id").distinct())

    if direction == "out":
        return edges_df.join(frontier, edges_df.src == frontier.id, "left_semi")

    if direction == "in":
        return edges_df.join(frontier, edges_df.dst == frontier.id, "left_semi")

    out_edges = edges_df.join(frontier, edges_df.src == frontier.id, "left_semi")
    in_edges = edges_df.join(frontier, edges_df.dst == frontier.id, "left_semi")
    return out_edges.unionByName(in_edges).dropDuplicates(["src", "dst"])


def _next_frontier(
    incident_edges: DataFrame,
    seen_nodes: DataFrame,
    direction: Literal["both", "out", "in"],
) -> DataFrame:
    if direction == "out":
        nxt = incident_edges.select(F.col("dst").alias("id"))
    elif direction == "in":
        nxt = incident_edges.select(F.col("src").alias("id"))
    else:
        nxt = incident_edges.select(F.col("src").alias("id")).unionByName(
            incident_edges.select(F.col("dst").alias("id"))
        )

    return nxt.distinct().join(seen_nodes.select("id").distinct(), "id", "left_anti")


def _ego_subgraph(
    seed_nodes: DataFrame,
    edges_df: DataFrame,
    vertices_df: DataFrame,
    hops: int,
    min_score: Optional[float],
    direction: Literal["both", "out", "in"],
    max_edges: Optional[int],
) -> tuple[DataFrame, DataFrame]:
    if hops <= 0:
        used_ids = seed_nodes.select("id").distinct()
        sub_vertices = vertices_df.join(used_ids, "id", "inner")
        return edges_df.limit(0), sub_vertices

    edges_pool = edges_df
    if min_score is not None:
        edges_pool = edges_pool.filter(F.col("score") >= F.lit(min_score))

    frontier = seed_nodes.select("id").distinct()
    seen_nodes = frontier
    collected_edges = edges_pool.limit(0)

    for _ in range(hops):
        incident = _incident_edges(edges_pool, frontier, direction)

        collected_edges = collected_edges.unionByName(incident).dropDuplicates(
            ["src", "dst"]
        )

        next_frontier = _next_frontier(incident, seen_nodes, direction)
        if _is_empty(next_frontier):
            break

        seen_nodes = seen_nodes.unionByName(next_frontier).dropDuplicates(["id"])
        frontier = next_frontier

    sub_edges = collected_edges

    if max_edges is not None:
        sub_edges = sub_edges.orderBy(
            F.col("score").desc(),
            F.col("last_interaction").desc(),
        ).limit(max_edges)

    used_ids_df = (
        sub_edges.select(F.col("src").alias("id"))
        .unionByName(sub_edges.select(F.col("dst").alias("id")))
        .unionByName(seen_nodes.select("id"))
        .dropDuplicates(["id"])
    )

    sub_vertices = vertices_df.join(used_ids_df, "id", "inner")
    return sub_edges, sub_vertices


@timed
def get_node_neighbors(
    node_id: str,
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    hops: int = 1,
    min_score: Optional[float] = None,
    direction: Literal["both", "out", "in"] = "both",
    max_edges: Optional[int] = None,
) -> tuple[DataFrame, DataFrame]:
    if edges_df is None or vertices_df is None:
        edges_df, vertices_df = get_edges_and_vertices()

    exists = vertices_df.filter(F.col("id") == F.lit(node_id)).limit(1)
    if _is_empty(exists):
        return _empty_like(edges_df), _empty_like(vertices_df)

    seed = spark.createDataFrame([(node_id,)], "id string")
    return _ego_subgraph(
        seed_nodes=seed,
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
        direction=direction,
        max_edges=max_edges,
    )


@timed
def get_edge_context(
    src: str,
    dst: str,
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    hops: int = 1,
    min_score: Optional[float] = None,
    max_edges: Optional[int] = None,
) -> tuple[DataFrame, DataFrame]:
    if edges_df is None or vertices_df is None:
        edges_df, vertices_df = get_edges_and_vertices()

    selected = edges_df.filter(
        (F.col("src") == F.lit(src)) & (F.col("dst") == F.lit(dst))
    )

    if _is_empty(selected):
        raise ValueError(f"Edge ({src}, {dst}) not found.")

    seed = spark.createDataFrame([(src,), (dst,)], "id string").distinct()

    sub_edges, _ = _ego_subgraph(
        seed_nodes=seed,
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
        direction="both",
        max_edges=None,
    )

    sub_edges = sub_edges.unionByName(selected).dropDuplicates(["src", "dst"])

    if max_edges is not None:
        sub_edges = (
            sub_edges.withColumn(
                "_priority",
                F.when(
                    (F.col("src") == F.lit(src)) & (F.col("dst") == F.lit(dst)),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .orderBy(
                F.col("_priority").desc(),
                F.col("score").desc(),
                F.col("last_interaction").desc(),
            )
            .limit(max_edges)
            .drop("_priority")
        )

    used_ids_df = (
        sub_edges.select(F.col("src").alias("id"))
        .unionByName(sub_edges.select(F.col("dst").alias("id")))
        .dropDuplicates(["id"])
    )

    sub_vertices = vertices_df.join(used_ids_df, "id", "inner")
    return sub_edges, sub_vertices


@timed
def get_best_edges(
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    limit: int = 100,
) -> tuple[DataFrame, DataFrame]:
    if edges_df is None or vertices_df is None:
        edges_df, vertices_df = get_edges_and_vertices()

    best_edges_df = (
        edges_df.select("src", "dst", "score", *EVENT_TYPE, "last_interaction")
        .orderBy(F.col("score").desc(), F.col("last_interaction").desc())
        .limit(limit)
    )

    used_ids_df = (
        best_edges_df.select(F.col("src").alias("id"))
        .unionByName(best_edges_df.select(F.col("dst").alias("id")))
        .distinct()
    )

    vertices_used_df = vertices_df.join(used_ids_df, "id", "inner")
    return best_edges_df, vertices_used_df


@timed
def to_obj(edges_df: DataFrame, vertices_df: DataFrame) -> dict:
    nodes = []
    for row in vertices_df.toLocalIterator():
        payload = row.asDict(recursive=True)
        node_id = payload["id"]
        label = payload.get("type") or node_id
        node_entry = {"id": node_id, "label": label}
        node_entry.update(payload)
        nodes.append(node_entry)

    links = []
    for row in edges_df.toLocalIterator():
        payload = row.asDict(recursive=True)
        src = payload.get("src")
        dst = payload.get("dst")
        edge_id = payload.get("id") or f"{src}__{dst}"
        link = {
            "id": edge_id,
            "source": src,
            "target": dst,
            "score": float(payload.get("score", 0.0)),
            "data": payload,
        }
        for et in EVENT_TYPE:
            if et in payload:
                link[et] = payload[et]
        links.append(link)

    return {"nodes": nodes, "links": links}
