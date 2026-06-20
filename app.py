from flask import Flask, jsonify, request, send_from_directory
from pyspark.sql import DataFrame   
from graph.graph import (
    get_node_neighbors,
    get_edge_context,
    get_best_edges,
    get_edges_and_vertices,
    get_graph_metrics,
    to_obj,
)
import os

app = Flask(__name__)







# ── API routes ──────────────────────────────────────────────────────────────


@app.route("/api/best-edges")
def api_best_edges():
    _edges_df, _vertices_df = get_edges_and_vertices()
    limit = int(request.args.get("limit", 100))
    edges, vertices = get_best_edges(_edges_df, _vertices_df, limit)
    return jsonify(to_obj(edges, vertices))


@app.route("/api/node/<node_id>")
def api_node_neighbors(node_id: str):
    _edges_df, _vertices_df = get_edges_and_vertices()
    hops = int(request.args.get("hops", 1))
    min_score = request.args.get("min_score", type=float, default=None)
    max_edges = request.args.get("max_edges", type=int, default=None)
    edges, vertices = get_node_neighbors(
        node_id,
        edges_df=_edges_df,
        vertices_df=_vertices_df,
        hops=hops,
        min_score=min_score,
        max_edges=max_edges,
    )
    return jsonify(to_obj(edges, vertices))


@app.route("/api/edge")
def api_edge_context():
    _edges_df, _vertices_df = get_edges_and_vertices()
    src = request.args.get("src")
    dst = request.args.get("dst")
    if not src or not dst:
        return jsonify({"error": "Both 'src' and 'dst' parameters are required."}), 400
    hops = int(request.args.get("hops", 1))
    min_score = request.args.get("min_score", type=float, default=None)
    max_edges = request.args.get("max_edges", type=int, default=None)
    edges, vertices = get_edge_context(
        src,
        dst,
        edges_df=_edges_df,
        vertices_df=_vertices_df,
        hops=hops,
        min_score=min_score,
        max_edges=max_edges,
    )
    return jsonify(to_obj(edges, vertices))


@app.route("/api/search")
def api_search_nodes():
    """Return all nodes matching a prefix query (for autocomplete)."""
    _edges_df, _vertices_df = get_edges_and_vertices()
    q = request.args.get("q", "").lower()
    if not q:
        return jsonify([])
    
    rows = (
        _vertices_df.filter(_vertices_df["id"].contains(q))
        .select("id", "type")
        .limit(20)
        .collect()
    )
    return jsonify([{"id": r["id"], "type": r["type"]} for r in rows])


@app.route("/api/refresh")
def api_refresh():
    """Reload edges and vertices from Delta tables."""
    global _edges_df, _vertices_df
    _edges_df, _vertices_df = get_edges_and_vertices()
    # best_egdes
    return api_best_edges()


@app.route("/api/stats")
def api_stats():
    """Return basic graph statistics."""
    _edges_df, _vertices_df = get_edges_and_vertices()
    assert _vertices_df is not None, "Vertices DataFrame should be loaded."
    assert _edges_df is not None, "Edges DataFrame should be loaded."
    n_vertices = _vertices_df.count()
    n_edges = _edges_df.count()
    return jsonify(
        {
            "vertices": n_vertices,
            "edges": n_edges,
        }
    )


@app.route("/api/graph-metrics")
def api_graph_metrics():
    """
    Indicateurs GraphFrames : centralité (PageRank) et composants connectés.

    Calcul coûteux (plusieurs itérations distribuées) — à appeler ponctuellement,
    pas à chaque tick d'auto-refresh du dashboard.
    """
    try:
        _edges_df, _vertices_df = get_edges_and_vertices()
        top_k = int(request.args.get("top_k", 10))
        metrics = get_graph_metrics(_edges_df, _vertices_df, top_k=top_k)
        return jsonify(metrics)
    except ImportError:
        return jsonify({
            "error": "GraphFrames n'est pas installé. "
                     "Vérifiez que le jar graphframes:graphframes:0.8.3-spark3.0-s_2.12 "
                     "est résolu dans spark.jars.packages (config.py)."
        }), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Serve the dashboard SPA ─────────────────────────────────────────────────


@app.route("/")
def index():
    return send_from_directory(
        os.path.join(app.root_path, "dashboard"), "index.html"
    )


@app.route("/assets/<path:filename>")
def static_files(filename):
    """Serve additional assets from dashboard/ if needed."""
    return send_from_directory(
        os.path.join(app.root_path, "dashboard"), filename
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
