from __future__ import annotations

from functools import reduce
from typing import Optional, Literal, Final, Any, Callable
from collections.abc import Iterator

import pyspark.sql.functions as F
from delta.tables import DeltaTable, ColumnMapping
from pyspark.sql import DataFrame
import pandas as pd
from pyspark.storagelevel import StorageLevel
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.streaming.state import GroupStateTimeout

from timer import timed
from config import (
    get_spark_session,
    RELATIONSHIP_SCORES,
    EVENT_TYPE,
    VERTICES_PATH,
    EDGES_PATH,
    EDGES_RAW_PATH,
    VERTICES_RAW_PATH,
    SAVE_RAW_DATA,
)

spark = get_spark_session()

AGGREGATION_FUNC = [
    F.sum(F.when(F.col("relationship") == event_type, 1).otherwise(0)).alias(event_type)
    for event_type in EVENT_TYPE
]

UPDATE_SET: ColumnMapping = {
    event_type: F.col(f"target.{event_type}") + F.col(f"source.{event_type}")
    for event_type in EVENT_TYPE
}
UPDATE_SET["last_interaction"] = F.greatest(
    F.col("target.last_interaction"), F.col("source.last_interaction")
)
UPDATE_SET["score"] = F.col("target.score") + F.col("source.score")

score_expr = reduce(
    lambda acc, et: acc + (F.col(et) * RELATIONSHIP_SCORES[et]),
    EVENT_TYPE[1:],
    F.col(EVENT_TYPE[0]) * RELATIONSHIP_SCORES[EVENT_TYPE[0]],
)

_edges_df: Optional[DataFrame] = None
_vertices_df: Optional[DataFrame] = None

_delta_table_cache: dict[str, DeltaTable | None] = {}

RAW_MATERIALIZER_CHECKPOINT_BASE: Final[str] = "/tmp/graph_materializer_checkpoint"


def _get_delta_table(path: str) -> DeltaTable | None:
    """Retourne le DeltaTable depuis le cache ; effectue le check filesystem une seule fois."""
    if path not in _delta_table_cache:
        _delta_table_cache[path] = (
            DeltaTable.forPath(spark, path)
            if DeltaTable.isDeltaTable(spark, path)
            else None
        )
    return _delta_table_cache[path]


def _register_delta_table(path: str) -> DeltaTable:
    """Enregistre dans le cache après une première écriture et retourne l'instance."""
    table = DeltaTable.forPath(spark, path)
    _delta_table_cache[path] = table
    return table


def invalidate_graph_cache() -> None:
    global _edges_df, _vertices_df
    
    # Unpersist old DataFrames to prevent memory leaks
    if _edges_df is not None:
        try:
            _edges_df.unpersist()
        except Exception:
            pass  # DataFrame may already be unpersisted
        _edges_df = None
    
    if _vertices_df is not None:
        try:
            _vertices_df.unpersist()
        except Exception:
            pass  # DataFrame may already be unpersisted
        _vertices_df = None


def _is_empty(df: DataFrame) -> bool:
    return len(df.head(1)) == 0


def _append_raw_delta(df: DataFrame, path: str, epoch_id: int) -> None:
    (
        df.withColumn("epoch_id", F.lit(epoch_id))
        .write.format("delta")
        .mode("append")
        .save(path)
    )


def _merge_vertices_snapshot(batch_df: DataFrame, batch_id: int) -> None:
    if _is_empty(batch_df):
        return

    vertices_table = _get_delta_table(VERTICES_PATH)
    if vertices_table is not None:
        (
            vertices_table.alias("t")
            .merge(batch_df.alias("s"), "t.id = s.id")
            .whenMatchedUpdate(set={"type": "s.type"})
            .whenNotMatchedInsert(values={"id": "s.id", "type": "s.type"})
            .execute()
        )
    else:
        batch_df.write.format("delta").mode("overwrite").save(VERTICES_PATH)
        _register_delta_table(VERTICES_PATH)


def _merge_edges_snapshot(batch_df: DataFrame, batch_id: int) -> None:
    if _is_empty(batch_df):
        return

    edges_table = _get_delta_table(EDGES_PATH)
    if edges_table is not None:
        (
            edges_table.alias("target")
            .merge(
                batch_df.alias("source"),
                "target.src = source.src AND target.dst = source.dst",
            )
            .whenMatchedUpdate(set=UPDATE_SET)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        batch_df.write.format("delta").mode("overwrite").save(EDGES_PATH)
        _register_delta_table(EDGES_PATH)


def _make_edge_state_update(
    relationship_scores: dict[str, float], event_types: list[str]
) -> Callable:
    """Factory function to avoid capturing large global variables in closure."""
    def _edge_state_update(
        key: tuple[str, str],
        pdf_iter: Iterator,
        state: Any,
    ) -> Iterator:
        # Import pandas locally to avoid capturing the entire pandas module in closure
        import pandas as pd
        src, dst = key
        scores = relationship_scores
        etypes = event_types

        if state.exists:
            count, last_interaction, last_epoch_id, score = state.get()
            count = int(count)
            last_epoch_id = int(last_epoch_id)
            score = float(score)
        else:
            count = 0
            last_interaction = None
            last_epoch_id = -1
            score = 0.0

        for pdf in pdf_iter:
            if pdf.empty:
                continue

            batch_count = int(len(pdf))
            batch_last_interaction = pdf["timestamp"].max()
            batch_epoch_id = int(pdf["epoch_id"].max()) if "epoch_id" in pdf.columns else last_epoch_id

            count += batch_count

            should_replace = False
            if last_interaction is None and pd.notna(batch_last_interaction):
                should_replace = True
            elif pd.notna(batch_last_interaction) and batch_last_interaction > last_interaction:
                should_replace = True
            elif pd.notna(batch_last_interaction) and batch_last_interaction == last_interaction:
                should_replace = batch_epoch_id >= last_epoch_id

            if should_replace:
                last_interaction = batch_last_interaction
                last_epoch_id = batch_epoch_id

            score = float(
                sum(count * scores[et] for et in etypes)
            )

        state.update((count, last_interaction, last_epoch_id, score))

        yield pd.DataFrame(
            [
                {
                    "src": src,
                    "dst": dst,
                    "count": count,
                    "last_interaction": last_interaction,
                    "score": score,
                    **{et: 0 for et in etypes},
                }
            ]
        )
    return _edge_state_update


def _make_vertex_state_update() -> Callable:
    """Factory function to avoid capturing global variables in closure."""
    def _vertex_state_update(
        key: tuple[str],
        pdf_iter: Iterator,
        state: Any,
    ) -> Iterator:
        # Import pandas locally to avoid capturing the entire pandas module in closure
        import pandas as pd
        (vertex_id,) = key

        if state.exists:
            current_type, last_epoch_id = state.get()
            last_epoch_id = int(last_epoch_id)
        else:
            current_type = None
            last_epoch_id = -1

        for pdf in pdf_iter:
            if pdf.empty:
                continue

            if "epoch_id" in pdf.columns:
                batch = pdf.sort_values("epoch_id")
                row = batch.iloc[-1]
                batch_epoch_id = int(row["epoch_id"])
            else:
                row = pdf.iloc[-1]
                batch_epoch_id = last_epoch_id

            if batch_epoch_id >= last_epoch_id:
                current_type = row["type"]
                last_epoch_id = batch_epoch_id

        state.update((current_type, last_epoch_id))

        yield pd.DataFrame(
            [
                {
                    "id": vertex_id,
                    "type": current_type,
                }
            ]
        )
    return _vertex_state_update


def start_graph_materializer() -> tuple[StreamingQuery, StreamingQuery]:
    """
    Lance un materializer stateful séparé.
    À appeler une seule fois dans un job dédié.
    """
    # Configure memory settings for streaming
    spark.conf.set(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    )
    spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2")  # Reduce memory usage
    spark.conf.set("spark.streaming.backpressure.enabled", "true")  # Enable backpressure
    
    # Use factory functions to avoid capturing global variables in closures
    # This significantly reduces the size of serialized task binaries
    edge_state_func = _make_edge_state_update(RELATIONSHIP_SCORES, EVENT_TYPE)
    vertex_state_func = _make_vertex_state_update()

    raw_edges_stream = (
        spark.readStream.format("delta")
        .load(EDGES_RAW_PATH)
        .select("src", "dst", "relationship", "timestamp", "epoch_id")
    )

    edges_state_df = raw_edges_stream.groupBy("src", "dst").applyInPandasWithState(
        edge_state_func,  # type: ignore[attr-defined]
        outputStructType="""
            src string,
            dst string,
            count long,
            last_interaction timestamp,
            score double,
            """ + ", ".join(f"{event_type} long" for event_type in EVENT_TYPE),
        stateStructType="""
            count long,
            last_interaction timestamp,
            last_epoch_id long,
            score double
        """,
        outputMode="update",
        timeoutConf=GroupStateTimeout.NoTimeout,
    )

    edges_query = (
        edges_state_df.writeStream.foreachBatch(_merge_edges_snapshot)
        .option("checkpointLocation", f"{RAW_MATERIALIZER_CHECKPOINT_BASE}/edges")
        .outputMode("update")
        .trigger(availableNow=True)
        .start()
    )

    raw_vertices_stream = (
        spark.readStream.format("delta")
        .load(VERTICES_RAW_PATH)
        .select("id", "type", "epoch_id")
    )

    vertices_state_df = raw_vertices_stream.groupBy("id").applyInPandasWithState(
        vertex_state_func,  # type: ignore[attr-defined]
        outputStructType="id string, type string",
        stateStructType="type string, last_epoch_id long",
        outputMode="update",
        timeoutConf=GroupStateTimeout.NoTimeout,
    )

    vertices_query = (
        vertices_state_df.writeStream.foreachBatch(_merge_vertices_snapshot)
        .option("checkpointLocation", f"{RAW_MATERIALIZER_CHECKPOINT_BASE}/vertices")
        .outputMode("update")
        .trigger(availableNow=True)
        .start()
    )

    return edges_query, vertices_query

def _empty_like(df: DataFrame) -> DataFrame:
    return df.limit(0)

def get_edges_and_vertices(force_reload: bool = False, persist: bool = False) -> tuple[DataFrame, DataFrame]:
    """
    Charge les tables Delta une seule fois et les garde en mémoire côté Spark.
    
    Args:
        force_reload: Si True, recharge les données depuis le stockage
        persist: Si True, persiste les DataFrames en mémoire (par défaut False pour éviter les fuites mémoire)
    """
    global _edges_df, _vertices_df

    if force_reload:
        invalidate_graph_cache()

    if _edges_df is None:
        read_op = spark.read.format("delta").load(EDGES_PATH).select("src", "dst", "score", "last_interaction", *EVENT_TYPE)
        _edges_df = read_op.persist(StorageLevel.MEMORY_AND_DISK) if persist else read_op

    if _vertices_df is None:
        read_op = spark.read.format("delta").load(VERTICES_PATH).select("id", "type")
        _vertices_df = read_op.persist(StorageLevel.MEMORY_AND_DISK) if persist else read_op

    return _edges_df, _vertices_df


@timed
def refresh_edges_and_vertices() -> tuple[DataFrame, DataFrame]:
    """Force a reload of the Delta-backed graph tables."""
    invalidate_graph_cache()
    return get_edges_and_vertices(force_reload=True)


@timed
def handle_new_data(
    new_vertices_df: DataFrame, new_edges_df: DataFrame, epoch_id: int
) -> None:
    """
    Hot path minimal:
    - écrit uniquement les données brutes si demandé
    - n'effectue plus de MERGE sur les tables de service
    """
    invalidate_graph_cache()

    if SAVE_RAW_DATA:
        _append_raw_delta(new_edges_df, EDGES_RAW_PATH, epoch_id)
        _append_raw_delta(new_vertices_df, VERTICES_RAW_PATH, epoch_id)


@timed
def get_best_edges(
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    limit: int = 100,
) -> tuple[DataFrame, DataFrame]:
    """Return the highest-scoring edges and the related vertices."""
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


def _incident_edges(
    edges_df: DataFrame,
    frontier_df: DataFrame,
    direction: Literal["both", "out", "in"],
) -> DataFrame:
    """
    Retourne les arêtes incidentes à une frontière de nœuds,
    sans jointure non équi (`OR`).
    """
    frontier = F.broadcast(frontier_df.select("id").dropDuplicates(["id"]))

    if direction == "out":
        return edges_df.join(frontier, edges_df.src == frontier.id, "left_semi")

    if direction == "in":
        return edges_df.join(frontier, edges_df.dst == frontier.id, "left_semi")

    out_edges = edges_df.join(frontier, edges_df.src == frontier.id, "left_semi")
    in_edges = edges_df.join(frontier, edges_df.dst == frontier.id, "left_semi")
    return out_edges.unionByName(in_edges).dropDuplicates(["src", "dst"])


def _next_frontier(
    incident_edges: DataFrame,
    current_frontier: DataFrame,
    direction: Literal["both", "out", "in"],
) -> DataFrame:
    """
    Calcule la nouvelle frontière à partir des arêtes incidentes.
    On enlève les nœuds déjà vus pour éviter les boucles inutiles.
    """
    if direction == "out":
        nxt = incident_edges.select(F.col("dst").alias("id"))
    elif direction == "in":
        nxt = incident_edges.select(F.col("src").alias("id"))
    else:
        nxt = (
            incident_edges.select(F.col("src").alias("id"))
            .unionByName(incident_edges.select(F.col("dst").alias("id")))
        )

    return nxt.dropDuplicates(["id"]).join(current_frontier, "id", "left_anti")

def _ego_subgraph(
    seed_nodes: DataFrame,
    edges_df: DataFrame,
    vertices_df: DataFrame,
    hops: int,
    min_score: Optional[float],
    direction: Literal["both", "out", "in"],
    max_edges: Optional[int],
) -> tuple[DataFrame, DataFrame]:
    """
    Cœur optimisé pour extraire un sous-graphe autour d'une frontière de départ.
    """
    if hops <= 0:
        used_ids = seed_nodes.dropDuplicates(["id"])
        sub_vertices = vertices_df.join(used_ids, "id", "inner")
        return edges_df.limit(0), sub_vertices

    edges_pool = edges_df
    if min_score is not None:
        edges_pool = edges_pool.filter(F.col("score") >= F.lit(min_score))

    frontier = seed_nodes.select("id").dropDuplicates(["id"])
    seen_nodes = frontier
    collected_edges = edges_pool.limit(0)

    for _ in range(hops):
        incident = _incident_edges(edges_pool, frontier, direction)
        collected_edges = collected_edges.unionByName(incident).dropDuplicates(["src", "dst"])

        frontier = _next_frontier(incident, seen_nodes, direction)
        if not frontier.take(1):
            break

        seen_nodes = seen_nodes.unionByName(frontier).dropDuplicates(["id"])

    sub_edges = collected_edges
    if max_edges is not None:
        sub_edges = sub_edges.orderBy(
            F.col("score").desc(),
            F.col("last_interaction").desc(),
        ).limit(max_edges)

    used_ids_df = (
        sub_edges.select(F.col("src").alias("id"))
        .unionByName(sub_edges.select(F.col("dst").alias("id")))
        .unionByName(seen_nodes)
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
    """
    Retourne le sous-graphe induit autour d’un nœud.
    Version optimisée : expansion BFS sur la frontière courante,
    sans jointures `OR` et sans `collect()` inutile.
    """
    if edges_df is None or vertices_df is None:
        edges_df, vertices_df = get_edges_and_vertices()

    exists = vertices_df.filter(F.col("id") == F.lit(node_id)).select("id").take(1)
    if not exists:
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
    """
    Contexte autour d'une arête (src -> dst).
    Optimisation importante : on calcule le voisinage des deux extrémités
    en une seule passe, pas deux.
    """
    if edges_df is None or vertices_df is None:
        edges_df, vertices_df = get_edges_and_vertices()

    selected = edges_df.filter(
        (F.col("src") == F.lit(src)) & (F.col("dst") == F.lit(dst))
    )

    if not selected.take(1):
        raise ValueError(f"Edge ({src}, {dst}) not found.")

    seed = spark.createDataFrame([(src,), (dst,)], "id string").dropDuplicates(["id"])

    sub_edges, sub_vertices = _ego_subgraph(
        seed_nodes=seed,
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
        direction="both",
        max_edges=max_edges,
    )

    sub_edges = sub_edges.unionByName(selected).dropDuplicates(["src", "dst"])
    if max_edges is not None:
        sub_edges = sub_edges.orderBy(
            F.col("score").desc(),
            F.col("last_interaction").desc(),
        ).limit(max_edges)

    used_ids_df = (
        sub_edges.select(F.col("src").alias("id"))
        .unionByName(sub_edges.select(F.col("dst").alias("id")))
        .unionByName(sub_vertices.select("id"))
        .dropDuplicates(["id"])
    )

    sub_vertices = vertices_df.join(used_ids_df, "id", "inner")
    return sub_edges, sub_vertices

@timed
def to_obj(edges_df: DataFrame, vertices_df: DataFrame) -> dict:
    """
    Convert Spark edges/vertices into a simple JSON-like object.

    This is useful for exporting to JS/HTML viewers.
    """
    nodes = []
    for row in vertices_df.toLocalIterator():
        payload = row.asDict()
        node_id = payload["id"]
        label = payload.get("label", node_id) or node_id
        node_entry = {"id": node_id, "label": label}
        node_entry.update(payload)
        nodes.append(node_entry)

    links = []
    for row in edges_df.toLocalIterator():
        payload = row.asDict()
        src = payload.get("src")
        dst = payload.get("dst")
        edge_id = payload.get("id") or f"{src}__{dst}"
        link = {
            "id": edge_id,
            "source": src,
            "target": dst,
            "score": float(payload.get("score", 0.0)),
            "title": "",
            "data": payload,
        }
        for et in EVENT_TYPE:
            if et in payload:
                link[et] = payload[et]
        links.append(link)

    return {"nodes": nodes, "links": links}


# ----------------------------
# Visualization helpers
# ----------------------------

def _collect_vertices(vertices_df: DataFrame) -> list[dict]:
    return [row.asDict(recursive=True) for row in vertices_df.toLocalIterator()]


def _collect_edges(edges_df: DataFrame) -> list[dict]:
    return [row.asDict(recursive=True) for row in edges_df.toLocalIterator()]


@timed
def build_networkx_graph(edges_df: DataFrame, vertices_df: DataFrame):
    """
    Build a networkx graph from Spark data.
    Returns None if networkx is not installed.
    """
    try:
        import networkx as nx
    except ImportError:
        return None

    G = nx.DiGraph()

    vertex_rows = _collect_vertices(vertices_df)
    edge_rows = _collect_edges(edges_df)

    for v in vertex_rows:
        node_id = v["id"]
        attrs = dict(v)
        attrs.setdefault("label", node_id)
        G.add_node(node_id, **attrs)

    for e in edge_rows:
        attrs = dict(e)
        src = attrs.pop("src")
        dst = attrs.pop("dst")
        G.add_edge(src, dst, **attrs)

    return G


def _add_nodes_edges_to_pyvis(net, edges_df: DataFrame, vertices_df: DataFrame, title_field: str = "type"):
    vertex_rows = _collect_vertices(vertices_df)
    edge_rows = _collect_edges(edges_df)

    degree = {}
    for e in edge_rows:
        degree[e["src"]] = degree.get(e["src"], 0) + 1
        degree[e["dst"]] = degree.get(e["dst"], 0) + 1

    vertex_map = {v["id"]: v for v in vertex_rows}

    for node_id, attrs in vertex_map.items():
        label = attrs.get("label", node_id)
        node_type = attrs.get(title_field) or attrs.get("type") or ""
        title = f"id: {node_id}"
        if node_type:
            title += f"<br>type: {node_type}"
        net.add_node(
            node_id,
            label=label,
            title=title,
            value=max(1, degree.get(node_id, 1)),
        )

    for e in edge_rows:
        title_lines = [f"{k}: {v}" for k, v in e.items() if k not in {"src", "dst"}]
        net.add_edge(
            e["src"],
            e["dst"],
            value=float(e.get("score", 1.0)),
            title="<br>".join(title_lines) if title_lines else None,
        )


@timed
def visualize_graph_html(
    edges_df: DataFrame,
    vertices_df: DataFrame,
    output_path: str = "graph.html",
    height: str = "800px",
    width: str = "100%",
    notebook: bool = False,
    directed: bool = True,
    physics: bool = True,
):
    """
    Render the graph into an interactive HTML file using pyvis.

    Returns the output_path. The graph is best used on subgraphs, not the full dataset.
    """
    try:
        from pyvis.network import Network
    except ImportError as exc:
        raise ImportError(
            "pyvis is required for interactive HTML visualization. Install it with: pip install pyvis"
        ) from exc

    net = Network(height=height, width=width, directed=directed, notebook=notebook)
    net.barnes_hut() if physics else net.toggle_physics(False)

    _add_nodes_edges_to_pyvis(net, edges_df, vertices_df)
    net.show(output_path)
    return output_path


@timed
def visualize_best_edges(
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    limit: int = 100,
    output_path: str = "best_edges.html",
):
    """Convenience wrapper: visualise the top-scoring edges."""
    best_edges_df, used_vertices_df = get_best_edges(edges_df, vertices_df, limit=limit)
    return visualize_graph_html(best_edges_df, used_vertices_df, output_path=output_path)


@timed
def visualize_node(
    node_id: str,
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    hops: int = 1,
    output_path: str = "node_neighborhood.html",
    min_score: Optional[float] = None,
):
    """Visualise a node and its neighbourhood."""
    sub_edges, sub_vertices = get_node_neighbors(
        node_id,
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
    )
    return visualize_graph_html(sub_edges, sub_vertices, output_path=output_path)


@timed
def visualize_edge(
    src: str,
    dst: str,
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    hops: int = 1,
    output_path: str = "edge_context.html",
    min_score: Optional[float] = None,
):
    """Visualise one edge and the neighbourhood around its endpoints."""
    sub_edges, sub_vertices = get_edge_context(
        src,
        dst,
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
    )
    return visualize_graph_html(sub_edges, sub_vertices, output_path=output_path)


# ----------------------------
# Optional helper to generate a compact summary for UI use
# ----------------------------

@timed
def summarise_selection(
    edges_df: DataFrame,
    vertices_df: DataFrame,
    selected_node: Optional[str] = None,
    selected_edge: Optional[tuple[str, str]] = None,
    top_k_edges: int = 10,
) -> dict:
    """
    Produce a small payload for an app/UI:
    - top edges
    - selected node context
    - selected edge context
    """
    result = {}

    best_edges_df, best_vertices_df = get_best_edges(edges_df, vertices_df, limit=top_k_edges)
    result["best_edges"] = to_obj(best_edges_df, best_vertices_df)

    if selected_node is not None:
        node_edges_df, node_vertices_df = get_node_neighbors(
            selected_node, edges_df=edges_df, vertices_df=vertices_df, hops=1
        )
        result["selected_node"] = {
            "node_id": selected_node,
            "graph": to_obj(node_edges_df, node_vertices_df),
        }

    if selected_edge is not None:
        src, dst = selected_edge
        edge_edges_df, edge_vertices_df = get_edge_context(
            src, dst, edges_df=edges_df, vertices_df=vertices_df, hops=1
        )
        result["selected_edge"] = {
            "src": src,
            "dst": dst,
            "graph": to_obj(edge_edges_df, edge_vertices_df),
        }

    return result