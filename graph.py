from __future__ import annotations

from functools import reduce
from typing import Optional, Literal

import pyspark.sql.functions as F
from delta.tables import DeltaTable, ColumnMapping
from pyspark.sql import DataFrame

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
    event_type: (F.col(f"target.{event_type}") + F.col(f"source.{event_type}"))
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
    _edges_df = None
    _vertices_df = None


def get_edges_and_vertices(force_reload: bool = False) -> tuple[DataFrame, DataFrame]:
    global _edges_df, _vertices_df
    if force_reload or _edges_df is None:
        _edges_df = spark.read.format("delta").load(EDGES_PATH)
    if force_reload or _vertices_df is None:
        _vertices_df = spark.read.format("delta").load(VERTICES_PATH)
    return _edges_df, _vertices_df


def refresh_edges_and_vertices() -> tuple[DataFrame, DataFrame]:
    """Force a reload of the Delta-backed graph tables."""
    invalidate_graph_cache()
    return get_edges_and_vertices(force_reload=True)


def handle_new_data(
    new_vertices_df: DataFrame, new_edges_df: DataFrame, epoch_id: int
) -> None:
    """Insert/merge new vertices and edges into Delta tables."""
    from pyspark.storagelevel import StorageLevel

    invalidate_graph_cache()

    if SAVE_RAW_DATA:
        new_vertices_df = new_vertices_df.persist(StorageLevel.MEMORY_AND_DISK)
        new_edges_df = new_edges_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        if SAVE_RAW_DATA:
            new_edges_df.withColumn("epoch_id", F.lit(epoch_id)).write.format("delta").mode(
                "append"
            ).save(EDGES_RAW_PATH)

            new_vertices_df.withColumn("epoch_id", F.lit(epoch_id)).write.format("delta").mode(
                "append"
            ).save(VERTICES_RAW_PATH)

        assert score_expr is not None, "score_expr should not be None"
        batch_edges_agg_df = (
            new_edges_df.groupBy("src", "dst")
            .agg(*AGGREGATION_FUNC, F.max("timestamp").alias("last_interaction"))
            .withColumn("score", score_expr)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        try:
            # ── Vertices ─────────────────────────────────────────────────────
            vertices_table = _get_delta_table(VERTICES_PATH)
            if vertices_table is not None:
                (
                    vertices_table.alias("t")
                    .merge(new_vertices_df.alias("s"), "t.id = s.id")
                    .whenMatchedUpdate(set={"type": "s.type"})
                    .whenNotMatchedInsert(values={"id": "s.id", "type": "s.type"})
                    .execute()
                )
            else:
                new_vertices_df.write.format("delta").mode("overwrite").save(VERTICES_PATH)
                _register_delta_table(VERTICES_PATH)

            # ── Edges ─────────────────────────────────────────────────────────
            edges_table = _get_delta_table(EDGES_PATH)
            if edges_table is not None:
                (
                    edges_table.alias("target")
                    .merge(
                        batch_edges_agg_df.alias("source"),
                        "target.src = source.src AND target.dst = source.dst",
                    )
                    .whenMatchedUpdate(set=UPDATE_SET)
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                batch_edges_agg_df.write.format("delta").mode("overwrite").save(EDGES_PATH)
                _register_delta_table(EDGES_PATH)

        finally:
            batch_edges_agg_df.unpersist()
    finally:
        if SAVE_RAW_DATA:
            new_vertices_df.unpersist()
            new_edges_df.unpersist()


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


def _expand_frontier(
    edges_df: DataFrame,
    frontier_df: DataFrame,
    direction: Literal["both", "out", "in"],
) -> DataFrame:
    """Return the next layer of node ids from a frontier."""
    frontier = F.broadcast(frontier_df)

    if direction == "out":
        return (
            edges_df.join(frontier, edges_df.src == frontier.id, "inner")
            .select(edges_df.dst.alias("id"))
            .distinct()
        )
    if direction == "in":
        return (
            edges_df.join(frontier, edges_df.dst == frontier.id, "inner")
            .select(edges_df.src.alias("id"))
            .distinct()
        )

    out_nodes = (
        edges_df.join(frontier, edges_df.src == frontier.id, "inner")
        .select(edges_df.dst.alias("id"))
    )
    in_nodes = (
        edges_df.join(frontier, edges_df.dst == frontier.id, "inner")
        .select(edges_df.src.alias("id"))
    )
    return out_nodes.unionByName(in_nodes).distinct()


def _edges_touching_nodes(
    edges_df: DataFrame,
    nodes_df: DataFrame,
    direction: Literal["both", "out", "in"],
) -> DataFrame:
    """Return edges connected to a node frontier, respecting direction."""
    nodes = F.broadcast(nodes_df)

    if direction == "out":
        return edges_df.join(nodes, edges_df.src == nodes.id, "inner").select(
            *[F.col(c) for c in edges_df.columns]
        )
    if direction == "in":
        return edges_df.join(nodes, edges_df.dst == nodes.id, "inner").select(
            *[F.col(c) for c in edges_df.columns]
        )

    return edges_df.join(
        nodes,
        (edges_df.src == nodes.id) | (edges_df.dst == nodes.id),
        "inner",
    ).select(*[F.col(c) for c in edges_df.columns])


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
    Return the induced subgraph around a node.

    hops=1 returns direct neighbours. hops>1 expands through repeated neighbour expansion.
    """
    if edges_df is None or vertices_df is None:
        edges_df, vertices_df = get_edges_and_vertices()

    frontier = vertices_df.filter(F.col("id") == node_id).select("id")
    if frontier.limit(1).collect() == []:
        empty_edges = edges_df.limit(0)
        empty_vertices = vertices_df.limit(0)
        return empty_edges, empty_vertices

    filtered_edges = edges_df
    if min_score is not None:
        filtered_edges = filtered_edges.filter(F.col("score") >= F.lit(min_score))

    if direction == "out":
        filtered_edges = filtered_edges.filter(F.col("src") == F.lit(node_id))
    elif direction == "in":
        filtered_edges = filtered_edges.filter(F.col("dst") == F.lit(node_id))
    else:
        filtered_edges = filtered_edges.filter(
            (F.col("src") == F.lit(node_id)) | (F.col("dst") == F.lit(node_id))
        )

    if hops <= 1:
        sub_edges = filtered_edges
        if max_edges is not None:
            sub_edges = sub_edges.orderBy(F.col("score").desc(), F.col("last_interaction").desc()).limit(max_edges)
        used_ids_df = (
            sub_edges.select(F.col("src").alias("id"))
            .unionByName(sub_edges.select(F.col("dst").alias("id")))
            .unionByName(frontier)
            .distinct()
        )
        sub_vertices = vertices_df.join(used_ids_df, "id", "inner")
        return sub_edges, sub_vertices

    current_nodes = frontier
    collected_edges = filtered_edges

    for _ in range(hops - 1):
        next_nodes = _expand_frontier(edges_df, current_nodes, direction)
        if min_score is not None:
            next_edges_pool = edges_df.filter(F.col("score") >= F.lit(min_score))
        else:
            next_edges_pool = edges_df

        next_edges = _edges_touching_nodes(next_edges_pool, next_nodes, direction)
        collected_edges = collected_edges.unionByName(next_edges).dropDuplicates(
            ["src", "dst"]
        )
        current_nodes = current_nodes.unionByName(next_nodes).distinct()

    sub_edges = collected_edges
    if max_edges is not None:
        sub_edges = sub_edges.orderBy(F.col("score").desc(), F.col("last_interaction").desc()).limit(max_edges)

    used_ids_df = (
        sub_edges.select(F.col("src").alias("id"))
        .unionByName(sub_edges.select(F.col("dst").alias("id")))
        .unionByName(frontier)
        .distinct()
    )
    sub_vertices = vertices_df.join(used_ids_df, "id", "inner")
    return sub_edges, sub_vertices


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
    Return a subgraph centered on an edge (src -> dst).

    The output includes the selected edge and the neighbourhood around its two endpoints.
    """
    if edges_df is None or vertices_df is None:
        edges_df, vertices_df = get_edges_and_vertices()

    selected = edges_df.filter((F.col("src") == F.lit(src)) & (F.col("dst") == F.lit(dst)))

    if selected.limit(1).collect() == []:
        raise ValueError(f"Edge ({src}, {dst}) not found.")

    node_a_edges, node_a_vertices = get_node_neighbors(
        src,
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
        max_edges=max_edges,
    )
    node_b_edges, node_b_vertices = get_node_neighbors(
        dst,
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
        max_edges=max_edges,
    )

    sub_edges = (
        node_a_edges.unionByName(node_b_edges).unionByName(selected).dropDuplicates(["src", "dst"])
    )
    if max_edges is not None:
        sub_edges = sub_edges.orderBy(F.col("score").desc(), F.col("last_interaction").desc()).limit(max_edges)

    used_ids_df = (
        sub_edges.select(F.col("src").alias("id"))
        .unionByName(sub_edges.select(F.col("dst").alias("id")))
        .unionByName(node_a_vertices.select("id"))
        .unionByName(node_b_vertices.select("id"))
        .distinct()
    )
    sub_vertices = vertices_df.join(used_ids_df, "id", "inner")
    return sub_edges, sub_vertices


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


def visualize_best_edges(
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    limit: int = 100,
    output_path: str = "best_edges.html",
):
    """Convenience wrapper: visualise the top-scoring edges."""
    best_edges_df, used_vertices_df = get_best_edges(edges_df, vertices_df, limit=limit)
    return visualize_graph_html(best_edges_df, used_vertices_df, output_path=output_path)


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