from typing import Optional
from pathlib import Path
import json
import threading
from pyspark.sql import DataFrame, Row
import pyspark.sql.functions as F
from dataclasses import dataclass, field
from pyspark.storagelevel import StorageLevel
from delta.tables import DeltaTable

from config import get_spark_session, SAVE_RAW_DATA, RELATIONSHIP_SCORES, EVENT_TYPE
from graph.schema import edges_schema, vertices_schema
from graph.utils import _is_empty, _append_raw_delta
from config import EDGES_PATH, VERTICES_PATH, EDGES_RAW_PATH, VERTICES_RAW_PATH

spark = get_spark_session()

@dataclass
class _EdgeState:
    counts: dict[str, int] = field(default_factory=lambda: {et: 0 for et in EVENT_TYPE})
    last_interaction: Optional[object] = None
    score: float = 0.0


@dataclass
class _VertexState:
    type: Optional[str] = None
    epoch_id: int = -1

class GraphStore:
    """
    État applicatif du graphe.
    - mise à jour en mémoire
    - checkpoint complet sur disque
    - reload depuis snapshot + replay raw après checkpoint
    """

    def __init__(self, checkpoint_dir: str) -> None:
        self.dir = Path(checkpoint_dir)
        self.dir.mkdir(parents=True, exist_ok=True)

        self.edges: dict[tuple[str, str], _EdgeState] = {}
        self.vertices: dict[str, _VertexState] = {}

        self.edges_df: Optional[DataFrame] = None
        self.vertices_df: Optional[DataFrame] = None

        self.batch_count: int = 0
        self.last_epoch_id: int = -1
        self.dirty: bool = False
        
        # Reentrant lock to protect concurrent access to edges and vertices dictionaries
        # and allow nested locking within the same thread (e.g., apply_batch -> checkpoint -> get_dataframes)
        self._lock = threading.RLock()

    def invalidate_cache(self) -> None:
        # Unpersisting DataFrames doesn't require the lock, but clearing the references does
        # to prevent race conditions with concurrent refresh_dataframes calls
        with self._lock:
            if self.edges_df is not None:
                try:
                    self.edges_df.unpersist()
                except Exception:
                    pass
                self.edges_df = None

            if self.vertices_df is not None:
                try:
                    self.vertices_df.unpersist()
                except Exception:
                    pass
                self.vertices_df = None

    def _recompute_edge_score(self, st: _EdgeState) -> None:
        st.score = float(
            sum(
                int(st.counts.get(et, 0)) * float(RELATIONSHIP_SCORES[et])
                for et in EVENT_TYPE
            )
        )

    def _update_edge(self, src: str, dst: str, rel: str, ts) -> None:
        if rel not in RELATIONSHIP_SCORES:
            return

        with self._lock:
            key = (src, dst)
            st = self.edges.get(key)
            if st is None:
                st = _EdgeState()
                self.edges[key] = st

            st.counts[rel] = int(st.counts.get(rel, 0)) + 1

            if ts is not None and (st.last_interaction is None or ts > st.last_interaction):
                st.last_interaction = ts

            self._recompute_edge_score(st)

    def _update_vertex(self, vid: str, vtype: Optional[str], epoch: int) -> None:
        with self._lock:
            st = self.vertices.get(vid)
            if st is None:
                st = _VertexState()
                self.vertices[vid] = st

            if epoch >= st.epoch_id:
                st.type = vtype
                st.epoch_id = epoch

    def _build_edges_df(self) -> DataFrame:
        with self._lock:
            edges_snapshot = list(self.edges.items())
        
        rows = []
        for (src, dst), st in edges_snapshot:
            row = {
                "src": src,
                "dst": dst,
                "score": float(st.score),
                "last_interaction": st.last_interaction,
            }
            row.update({et: int(st.counts.get(et, 0)) for et in EVENT_TYPE})
            rows.append(row)

        if not rows:
            return spark.createDataFrame([], schema=edges_schema())

        return spark.createDataFrame(rows, schema=edges_schema())

    def _build_vertices_df(self) -> DataFrame:
        with self._lock:
            vertices_snapshot = list(self.vertices.items())
        
        rows = [
            Row(id=vid, type=st.type)
            for vid, st in vertices_snapshot
        ]

        if not rows:
            return spark.createDataFrame([], schema=vertices_schema())
        
        return spark.createDataFrame(rows, schema=vertices_schema())

    def refresh_dataframes(self) -> tuple[DataFrame, DataFrame]:
        # invalidate_cache already acquires the lock, so we need to be careful
        # We'll let invalidate_cache handle its own locking
        self.invalidate_cache()
        # _build_edges_df and _build_vertices_df acquire the lock internally
        self.edges_df = self._build_edges_df().persist(StorageLevel.MEMORY_AND_DISK)
        self.vertices_df = self._build_vertices_df().persist(StorageLevel.MEMORY_AND_DISK)
        return self.edges_df, self.vertices_df

    def get_dataframes(self, persist: bool = False) -> tuple[DataFrame, DataFrame]:
        # Check if we need to refresh - use lock to get consistent view
        with self._lock:
            need_refresh = self.edges_df is None or self.vertices_df is None
        
        if need_refresh:
            self.refresh_dataframes()

        assert self.edges_df is not None
        assert self.vertices_df is not None

        if persist:
            # Note: persist() is a Spark operation that doesn't modify our dictionaries
            # so we don't need the lock here
            self.edges_df = self.edges_df.persist(StorageLevel.MEMORY_AND_DISK)
            self.vertices_df = self.vertices_df.persist(StorageLevel.MEMORY_AND_DISK)

        return self.edges_df, self.vertices_df

    def checkpoint(self) -> None:
        edges_df, vertices_df = self.get_dataframes(persist=False)

        edges_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(EDGES_PATH)

        vertices_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(VERTICES_PATH)

        meta_path = self.dir / "meta.json"
        with self._lock:
            batch_count = self.batch_count
            last_epoch_id = self.last_epoch_id
        
        meta_path.write_text(
            json.dumps(
                {
                    "batch_count": batch_count,
                    "last_epoch_id": last_epoch_id,
                },
                ensure_ascii=False,
            )
        )

        with self._lock:
            self.dirty = False

    def _load_snapshot_tables(self) -> bool:
        loaded_any = False

        if DeltaTable.isDeltaTable(spark, EDGES_PATH):
            df = spark.read.format("delta").load(EDGES_PATH)
            if not _is_empty(df):
                loaded_any = True
                for r in df.toLocalIterator():
                    st = _EdgeState()
                    st.score = float(r["score"]) if r["score"] is not None else 0.0
                    st.last_interaction = r["last_interaction"]

                    payload = r.asDict(recursive=True)
                    for et in EVENT_TYPE:
                        st.counts[et] = int(payload.get(et, 0) or 0)

                    with self._lock:
                        self.edges[(r["src"], r["dst"])] = st

        if DeltaTable.isDeltaTable(spark, VERTICES_PATH):
            df = spark.read.format("delta").load(VERTICES_PATH)
            if not _is_empty(df):
                loaded_any = True
                for r in df.toLocalIterator():
                    with self._lock:
                        self.vertices[r["id"]] = _VertexState(
                            type=r["type"],
                            epoch_id=-1,
                        )

        return loaded_any

    def _replay_raw_since_checkpoint(self, last_epoch_id: int) -> None:
        if SAVE_RAW_DATA and DeltaTable.isDeltaTable(spark, EDGES_RAW_PATH):
            raw_edges = spark.read.format("delta").load(EDGES_RAW_PATH).dropDuplicates(
                ["src", "dst", "relationship", "timestamp", "epoch_id"]
            )
            if last_epoch_id >= 0:
                raw_edges = raw_edges.filter(F.col("epoch_id") > F.lit(last_epoch_id))

            for r in raw_edges.toLocalIterator():
                # _update_edge acquires the lock internally
                self._update_edge(
                    src=r["src"],
                    dst=r["dst"],
                    rel=r["relationship"],
                    ts=r["timestamp"],
                )

        if SAVE_RAW_DATA and DeltaTable.isDeltaTable(spark, VERTICES_RAW_PATH):
            raw_vertices = spark.read.format("delta").load(VERTICES_RAW_PATH).dropDuplicates(
                ["id", "type", "epoch_id"]
            )
            if last_epoch_id >= 0:
                raw_vertices = raw_vertices.filter(F.col("epoch_id") > F.lit(last_epoch_id))

            for r in raw_vertices.toLocalIterator():
                # _update_vertex acquires the lock internally
                self._update_vertex(
                    vid=r["id"],
                    vtype=r["type"],
                    epoch=int(r["epoch_id"]),
                )

    def load_checkpoint(self) -> None:
        with self._lock:
            self.invalidate_cache()
            self.edges.clear()
            self.vertices.clear()
            self.batch_count = 0
            self.last_epoch_id = -1

        loaded_snapshot = self._load_snapshot_tables()

        meta_path = self.dir / "meta.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                with self._lock:
                    self.batch_count = int(meta.get("batch_count", 0))
                    self.last_epoch_id = int(meta.get("last_epoch_id", -1))
            except Exception:
                with self._lock:
                    self.batch_count = 0
                    self.last_epoch_id = -1

        if loaded_snapshot:
            self._replay_raw_since_checkpoint(self.last_epoch_id)
        elif SAVE_RAW_DATA:
            self._replay_raw_since_checkpoint(-1)

        self.refresh_dataframes()
        with self._lock:
            self.dirty = False

    def apply_batch(
        self,
        new_vertices_df: DataFrame,
        new_edges_df: DataFrame,
        epoch_id: int,
        checkpoint_every_n_batches: int = 20,
    ) -> None:
        """
        Applique un micro-batch :
        - append raw optionnel
        - update mémoire
        - refresh des DataFrames de service
        - checkpoint périodique
        """

        # Check epoch_id first to avoid unnecessary work
        with self._lock:
            if epoch_id <= self.last_epoch_id:
                return

        # Save raw data (doesn't modify our dictionaries)
        if SAVE_RAW_DATA:
            if new_edges_df is not None and not _is_empty(new_edges_df):
                _append_raw_delta(new_edges_df, EDGES_RAW_PATH, epoch_id, kind="edge")

            if new_vertices_df is not None and not _is_empty(new_vertices_df):
                _append_raw_delta(new_vertices_df, VERTICES_RAW_PATH, epoch_id, kind="vertex")

        # Update edges - _update_edge acquires the lock internally
        if new_edges_df is not None and not _is_empty(new_edges_df):
            for r in new_edges_df.select("src", "dst", "relationship", "timestamp").toLocalIterator():
                self._update_edge(
                    src=r["src"],
                    dst=r["dst"],
                    rel=r["relationship"],
                    ts=r["timestamp"],
                )

        # Update vertices - _update_vertex acquires the lock internally
        if new_vertices_df is not None and not _is_empty(new_vertices_df):
            prepared_vertices = new_vertices_df
            if "epoch_id" in prepared_vertices.columns:
                prepared_vertices = prepared_vertices.select("id", "type", "epoch_id")
            else:
                prepared_vertices = prepared_vertices.withColumn(
                    "epoch_id", F.lit(epoch_id).cast("long")
                ).select("id", "type", "epoch_id")

            for r in prepared_vertices.toLocalIterator():
                self._update_vertex(
                    vid=r["id"],
                    vtype=r["type"],
                    epoch=int(r["epoch_id"]),
                )

        # Update metadata and refresh
        with self._lock:
            self.last_epoch_id = epoch_id
            self.batch_count += 1
            self.dirty = True
        
        self.refresh_dataframes()

        # Checkpoint if needed
        with self._lock:
            if checkpoint_every_n_batches > 0 and self.batch_count % checkpoint_every_n_batches == 0:
                self.checkpoint()

    def flush(self) -> None:
        with self._lock:
            dirty = self.dirty
        if dirty:
            self.checkpoint()
