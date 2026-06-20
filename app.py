from pathlib import Path
import time
from flask import Flask, jsonify, request, send_from_directory
import pyspark.sql.functions as F
from graph import (
    get_node_neighbors,
    get_edge_context,
    get_best_edges,
    get_edges_and_vertices,
    invalidate_graph_cache,
    to_obj,
)
 
app = Flask(__name__)

# Simple in-memory cache without external dependencies
class SimpleCache:
    def __init__(self, timeout=10):
        self.cache = {}
        self.timeout = timeout
    
    def get(self, key):
        cached = self.cache.get(key)
        if cached and time.time() - cached['timestamp'] < self.timeout:
            return cached['value']
        return None
    
    def set(self, key, value):
        self.cache[key] = {'value': value, 'timestamp': time.time()}

cache = SimpleCache(timeout=10)


def cached_endpoint(f):
    """Decorator to cache endpoint responses for 10 seconds."""
    def decorated(*args, **kwargs):
        # Create cache key from function name and query string
        cache_key = f.__name__ + "?" + request.query_string.decode()
        
        # Check cache
        cached_response = cache.get(cache_key)
        if cached_response is not None:
            return cached_response
        
        # Execute function and cache result
        response = f(*args, **kwargs)
        cache.set(cache_key, response)
        return response
    return decorated


BASE_DIR = Path(__file__).resolve().parent


def _dashboard_root() -> Path:
    dashboard_dir = BASE_DIR / "dashboard"
    if (dashboard_dir / "index.html").exists():
        return dashboard_dir
    if (BASE_DIR / "index.html").exists():
        return BASE_DIR
    return dashboard_dir


def _force_reload() -> bool:
    return request.args.get("force_reload", "0") == "1"


@app.route("/api/best-edges")
def api_best_edges():
    edges_df, vertices_df = get_edges_and_vertices(force_reload=_force_reload())
    limit = int(request.args.get("limit", 100))
    edges, vertices = get_best_edges(edges_df, vertices_df, limit)
    return jsonify(to_obj(edges, vertices))


@app.route("/api/node/<node_id>")
def api_node_neighbors(node_id: str):
    edges_df, vertices_df = get_edges_and_vertices(force_reload=_force_reload())
    hops = int(request.args.get("hops", 1))
    min_score = request.args.get("min_score", type=float, default=None)
    max_edges = request.args.get("max_edges", type=int, default=None)
    edges, vertices = get_node_neighbors(
        node_id,
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
        max_edges=max_edges,
    )
    return jsonify(to_obj(edges, vertices))


@app.route("/api/edge")
def api_edge_context():
    edges_df, vertices_df = get_edges_and_vertices(force_reload=_force_reload())
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
        edges_df=edges_df,
        vertices_df=vertices_df,
        hops=hops,
        min_score=min_score,
        max_edges=max_edges,
    )
    return jsonify(to_obj(edges, vertices))


@app.route("/api/search")
def api_search_nodes():
    edges_df, vertices_df = get_edges_and_vertices(force_reload=_force_reload())
    q = request.args.get("q", "").strip().lower()
    if not q:
        return jsonify([])

    rows = (
        vertices_df.filter(F.lower(F.col("id")).startswith(q))
        .select("id", "type")
        .limit(20)
        .collect()
    )
    return jsonify([{"id": r["id"], "type": r["type"]} for r in rows])


@app.route("/api/stats")
def api_stats():
    edges_df, vertices_df = get_edges_and_vertices(force_reload=_force_reload())
    return jsonify(
        {
            "vertices": vertices_df.count(),
            "edges": edges_df.count(),
        }
    )


@app.route("/api/refresh")
def api_refresh():
    invalidate_graph_cache()
    return jsonify({"status": "success"})


@app.route("/")
def index():
    root = _dashboard_root()
    return send_from_directory(str(root), "index.html")


@app.route("/assets/<path:filename>")
def static_files(filename):
    root = _dashboard_root()
    return send_from_directory(str(root), filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
