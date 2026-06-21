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

    Thread-safety :
      _lock         : RLock protégeant les dicts edges/vertices et les métadonnées
                      (batch_count, last_epoch_id, dirty, edges_df, vertices_df).
      _refresh_lock : Lock non-réentrant garantissant qu'un seul rebuild de
                      DataFrames tourne à la fois et réalisant un swap atomique
                      (build → swap → clear dirty) sans jamais exposer None aux
                      threads lecteurs.
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

        # RLock : permet la réentrance intra-thread (apply_batch → checkpoint → get_dataframes)
        self._lock = threading.RLock()
        self._refresh_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def invalidate_cache(self) -> None:
        """
        Force the next get_dataframes() to rebuild the DataFrames.
        Used only by load_checkpoint(); does not set the DFs to None in a visible way to readers (refresh_dataframes does an atomic swap).
        """
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
            self.dirty = True

    # ------------------------------------------------------------------
    # In-memory state mutations
    # ------------------------------------------------------------------

    def _recompute_edge_score(self, st: _EdgeState) -> None:
        st.score = float(
            sum(
                int(st.counts.get(et, 0)) * float(RELATIONSHIP_SCORES[et])
                for et in EVENT_TYPE
            )
        )

    def _update_edge(self, src: str, dst: str, rel: str, ts) -> None:
        if rel not in RELATIONSHIP_SCORES:
            print(f"Warning: unknown relationship type '{rel}' encountered. Ignoring.")
            return

        with self._lock:
            key = (src, dst)
            st = self.edges.get(key)
            if st is None:
                st = _EdgeState()
                self.edges[key] = st

            st.counts[rel] = int(st.counts.get(rel, 0)) + 1

            if ts is not None and (
                st.last_interaction is None or ts > st.last_interaction
            ):
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

    # ------------------------------------------------------------------
    # DataFrame builders
    # ------------------------------------------------------------------

    def _build_edges_df(self) -> DataFrame:
        with self._lock:
            rows = [
                Row(
                    src=src,
                    dst=dst,
                    score=float(st.score),
                    last_interaction=st.last_interaction,
                    **{et: int(st.counts.get(et, 0)) for et in EVENT_TYPE},
                )
                for (src, dst), st in self.edges.items()
            ]

        if not rows:
            print(f"Warning: No edges to create DataFrame from. empty :{rows==[]}, none :{rows is None}, edges dict: {self.edges}")
            return spark.createDataFrame([], schema=edges_schema())

        return spark.createDataFrame(rows, schema=edges_schema())

    def _build_vertices_df(self) -> DataFrame:
        with self._lock:
            rows = [Row(id=vid, type=st.type) for vid, st in self.vertices.items()]

        if not rows:
            print(f"Warning: No vertices to create DataFrame from. empty :{rows==[]}, none :{rows is None}, vertices dict: {self.vertices}")
            return spark.createDataFrame([], schema=vertices_schema())

        return spark.createDataFrame(rows, schema=vertices_schema())

    # ------------------------------------------------------------------
    # DataFrame refresh (swap atomique)
    # ------------------------------------------------------------------

    def refresh_dataframes(self, force_refresh: bool = False) -> tuple[DataFrame, DataFrame]:
        """
        Reconstruit les DataFrames depuis l'état en mémoire.

        Quand force_refresh=True, on reconstruit même si le cache DataFrame
        semble déjà à jour. Cela sert pour les chemins de reload qui veulent
        ignorer un cache potentiellement périmé, sans relire le checkpoint disque.
        """
        old_edges_df: Optional[DataFrame] = None
        old_vertices_df: Optional[DataFrame] = None

        with self._refresh_lock:
            # double-check locking if another thread has already rebuilt the DataFrames
            with self._lock:
                if not force_refresh and not (
                    self.edges_df is None or self.vertices_df is None or self.dirty
                ):
                    return self.edges_df, self.vertices_df
            print(f"[DEBUG] edge len: {len(self.edges)}, vertex len: {len(self.vertices)}, dirty: {self.dirty}, force_refresh: {force_refresh}")
            # build new DataFrames (potentially expensive) outside of the lock to avoid blocking readers
            new_edges_df = self._build_edges_df().persist(StorageLevel.MEMORY_AND_DISK)
            new_vertices_df = self._build_vertices_df().persist(
                StorageLevel.MEMORY_AND_DISK
            )

            with self._lock:
                old_edges_df = self.edges_df
                old_vertices_df = self.vertices_df
                self.edges_df = new_edges_df
                self.vertices_df = new_vertices_df
                self.dirty = False
                print(f"[DEBUG] new edge len after refresh: {len(self.edges)}, vertex len: {len(self.vertices)}, dirty: {self.dirty}")
        # Unpersist the old DataFrames after releasing the locks
        if old_edges_df is not None:
            try:
                old_edges_df.unpersist()
            except Exception:
                pass
        if old_vertices_df is not None:
            try:
                old_vertices_df.unpersist()
            except Exception:
                pass

        return self.edges_df, self.vertices_df

    def get_dataframes(
        self, persist: bool = False, force_refresh: bool = False
    ) -> tuple[DataFrame, DataFrame]:
        print(f"[DEBUG] get_dataframes before any operation: edge len: {len(self.edges)}, vertex len: {len(self.vertices)}, dirty: {self.dirty}, force_refresh: {force_refresh}")
        # print(f"[DEBUG] get_dataframes before any operation is None: edges is None: {self.edges is None}, vertex df is None: {self.vertices is None}")
        with self._lock:
            need_refresh = (
                self.edges_df is None or self.vertices_df is None or self.dirty
            )

        if force_refresh or need_refresh:
            self.refresh_dataframes(force_refresh=force_refresh)

        assert self.edges_df is not None
        assert self.vertices_df is not None

        if persist:
            self.edges_df = self.edges_df.persist(StorageLevel.MEMORY_AND_DISK)
            self.vertices_df = self.vertices_df.persist(StorageLevel.MEMORY_AND_DISK)
        # print(f"[DEBUG] get_dataframes: edge len: {len(self.edges)}, vertex len: {len(self.vertices)}, dirty: {self.dirty}, force_refresh: {force_refresh}")
        # print(f"[DEBUG] get_dataframes: edge len in df: {self.edges_df.count()}, vertex len in df: {self.vertices_df.count()}")
        return self.edges_df, self.vertices_df

    # ------------------------------------------------------------------
    # Checkpoint (persistence Delta)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Snapshot + replay (load from disk)
    # ------------------------------------------------------------------

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
            raw_edges = (
                spark.read.format("delta")
                .load(EDGES_RAW_PATH)
                .dropDuplicates(["src", "dst", "relationship", "timestamp", "epoch_id"])
            )
            if last_epoch_id >= 0:
                raw_edges = raw_edges.filter(F.col("epoch_id") > F.lit(last_epoch_id))

            for r in raw_edges.toLocalIterator():
                self._update_edge(
                    src=r["src"],
                    dst=r["dst"],
                    rel=r["relationship"],
                    ts=r["timestamp"],
                )

        if SAVE_RAW_DATA and DeltaTable.isDeltaTable(spark, VERTICES_RAW_PATH):
            raw_vertices = (
                spark.read.format("delta")
                .load(VERTICES_RAW_PATH)
                .dropDuplicates(["id", "type", "epoch_id"])
            )
            if last_epoch_id >= 0:
                raw_vertices = raw_vertices.filter(
                    F.col("epoch_id") > F.lit(last_epoch_id)
                )

            for r in raw_vertices.toLocalIterator():
                self._update_vertex(
                    vid=r["id"],
                    vtype=r["type"],
                    epoch=int(r["epoch_id"]),
                )

    def load_checkpoint(self) -> None:
        with self._lock:
            self.invalidate_cache()  # met dirty=True, dfs=None
            # self.edges.clear()
            # self.vertices.clear()
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

        # refresh_dataframes remet dirty=False après le swap
        self.refresh_dataframes(force_refresh=True)

    # ------------------------------------------------------------------
    # Micro-batch application
    # ------------------------------------------------------------------

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
        # print(f"[DEBUG] number of new edges: {new_edges_df.count() if new_edges_df is not None else 0}, number of new vertices: {new_vertices_df.count() if new_vertices_df is not None else 0}, epoch_id: {epoch_id}")
        # print(f"[DEBUG] number of edges in memory: {len(self.edges)}, number of vertices in memory: {len(self.vertices)}")


        # Save raw data to Delta tables if enabled
        if SAVE_RAW_DATA:
            if new_edges_df is not None and not _is_empty(new_edges_df):
                _append_raw_delta(new_edges_df, EDGES_RAW_PATH, epoch_id, kind="edge")

            if new_vertices_df is not None and not _is_empty(new_vertices_df):
                _append_raw_delta(
                    new_vertices_df, VERTICES_RAW_PATH, epoch_id, kind="vertex"
                )

        # Mise à jour des arêtes
        if new_edges_df is not None and not _is_empty(new_edges_df):
            for r in new_edges_df.select(
                "src", "dst", "relationship", "timestamp"
            ).toLocalIterator():
                self._update_edge(
                    src=r["src"],
                    dst=r["dst"],
                    rel=r["relationship"],
                    ts=r["timestamp"],
                )
                # print(f"[DEBUG] updated edge: src={r['src']}, dst={r['dst']}, rel={r['relationship']}, ts={r['timestamp']}")

        # Mise à jour des sommets
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

        # Mise à jour des métadonnées et signal de rebuild
        with self._lock:
            self.last_epoch_id = epoch_id
            self.batch_count += 1
            self.dirty = True  # positionné APRÈS toutes les mutations de dicts

        # Rebuild les DataFrames (remet dirty=False via swap atomique)
        self.refresh_dataframes(force_refresh=True)

        with self._lock:
            should_checkpoint = (
                checkpoint_every_n_batches > 0
                and self.batch_count % checkpoint_every_n_batches == 0
            )

        if should_checkpoint:
            self.checkpoint()
        print(f"[DEBUG] apply_batch: number of edges in memory after batch: {len(self.edges)}, number of vertices in memory after batch: {len(self.vertices)}, dirty: {self.dirty}, epoch_id: {self.last_epoch_id}, batch_count: {self.batch_count}")
        

    def flush(self) -> None:
        with self._lock:
            dirty = self.dirty
        if dirty:
            self.checkpoint()