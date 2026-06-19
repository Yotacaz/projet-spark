
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

global _edges_df, _vertices_df
_edges_df: Optional[DataFrame] = None
_vertices_df: Optional[DataFrame] = None
def handle_new_data(
    new_vertices_df: DataFrame, new_edges_df: DataFrame, epoch_id: int
) -> None:
    """Insert/merge new vertices and edges into Delta tables."""
    global _edges_df, _vertices_df
    _edges_df = None
    _vertices_df = None
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
    )

    if DeltaTable.isDeltaTable(spark, VERTICES_PATH):
        vertices_table = DeltaTable.forPath(spark, VERTICES_PATH)
        (
            vertices_table.alias("t")
            .merge(new_vertices_df.alias("s"), "t.id = s.id")
            .whenMatchedUpdate(set={"type": "s.type"})
            .whenNotMatchedInsert(values={"id": "s.id", "type": "s.type"})
            .execute()
        )
    else:
        new_vertices_df.write.format("delta").mode("overwrite").save(VERTICES_PATH)

    if DeltaTable.isDeltaTable(spark, EDGES_PATH):
        edges_table = DeltaTable.forPath(spark, EDGES_PATH)
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


def get_edges_and_vertices() -> tuple[DataFrame, DataFrame]:
    """Return the Delta edges and vertices dataframes."""
    if not DeltaTable.isDeltaTable(spark, EDGES_PATH):
        raise RuntimeError("La table Delta des edges n'existe pas.")
    if not DeltaTable.isDeltaTable(spark, VERTICES_PATH):
        raise RuntimeError("La table Delta des vertices n'existe pas.")
    global _edges_df, _vertices_df
    if _edges_df is None:
        _edges_df = spark.read.format("delta").load(EDGES_PATH)
    if _vertices_df is None:
        _vertices_df = spark.read.format("delta").load(VERTICES_PATH)
    edges_df = _edges_df
    vertices_df = _vertices_df
    return edges_df, vertices_df


def get_best_edges(
    edges_df: Optional[DataFrame] = None,
    vertices_df: Optional[DataFrame] = None,
    limit: int = 100,
) -> tuple[DataFrame, DataFrame]:
    """Return the highest-scoring edges and the related vertices."""
    if edges_df is None or vertices_df is None:
        edges_df, vertices_df = get_edges_and_vertices()

    best_edges_df = (
        edges_df.orderBy(F.col("score").desc(), F.col("last_interaction").desc())
        .limit(limit)
        .select("src", "dst", "score", *EVENT_TYPE, "last_interaction")
    )

    used_ids_df = (
        best_edges_df.select(F.col("src").alias("id"))
        .unionByName(best_edges_df.select(F.col("dst").alias("id")))
        .distinct()
    )
    vertices_used_df = vertices_df.join(used_ids_df, "id", "inner")
    return best_edges_df, vertices_used_df


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

    sub_edges = edges_df
    frontier = vertices_df.filter(F.col("id") == node_id).select("id")

    if min_score is not None:
        sub_edges = sub_edges.filter(F.col("score") >= F.lit(min_score))

    if direction == "out":
        sub_edges = sub_edges.filter(F.col("src") == F.lit(node_id))
    elif direction == "in":
        sub_edges = sub_edges.filter(F.col("dst") == F.lit(node_id))
    else:
        sub_edges = sub_edges.filter((F.col("src") == F.lit(node_id)) | (F.col("dst") == F.lit(node_id)))

    if hops > 1:
        # Iteratively expand the set of vertices by joining on src/dst.
        current_nodes = frontier
        collected_edges = sub_edges

        for _ in range(hops - 1):
            next_nodes = (
                edges_df.join(current_nodes, edges_df.src == current_nodes.id, "inner")
                .select(edges_df.dst.alias("id"))
                .unionByName(
                    edges_df.join(current_nodes, edges_df.dst == current_nodes.id, "inner")
                    .select(edges_df.src.alias("id"))
                )
                .distinct()
            )
            if min_score is not None:
                next_edges = edges_df.filter(F.col("score") >= F.lit(min_score))
            else:
                next_edges = edges_df
            collected_edges = (
                collected_edges.unionByName(
                    next_edges.join(next_nodes, (next_edges.src == next_nodes.id) | (next_edges.dst == next_nodes.id), "inner")
                    .select(collected_edges.columns)
                )
                .dropDuplicates(["src", "dst"])
            )
            current_nodes = current_nodes.unionByName(next_nodes).distinct()

        sub_edges = collected_edges

    if max_edges is not None:
        sub_edges = sub_edges.orderBy(F.col("score").desc()).limit(max_edges)

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

    if selected.count() == 0:
        raise ValueError(f"Edge ({src}, {dst}) not found.")

    node_a_edges, node_a_vertices = get_node_neighbors(
        src, edges_df=edges_df, vertices_df=vertices_df, hops=hops, min_score=min_score, max_edges=max_edges
    )
    node_b_edges, node_b_vertices = get_node_neighbors(
        dst, edges_df=edges_df, vertices_df=vertices_df, hops=hops, min_score=min_score, max_edges=max_edges
    )

    sub_edges = node_a_edges.unionByName(node_b_edges).unionByName(selected).dropDuplicates(["src", "dst"])
    if max_edges is not None:
        sub_edges = sub_edges.orderBy(F.col("score").desc()).limit(max_edges)

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
    for row in vertices_df.select("id", *([c for c in vertices_df.columns if c != "id"])).toLocalIterator():
        label = row["id"]
        if "label" in row.asDict() and row["label"] is not None:
            label = row["label"]
        nodes.append(
            {
                "id": row["id"],
                "label": label,
                "type": row.asDict().get("type"),
            }
        )

    links = []
    for row in edges_df.collect():
        payload = row.asDict()
        links.append(
            {
                "source": payload["src"],
                "target": payload["dst"],
                "score": float(payload.get("score", 0.0)),
                "title": payload.get("relationship", ""),
                "data": payload,
            }
        )

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

    # Degree from the subgraph for sizing.
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
