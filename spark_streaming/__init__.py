"""
Réexporte les symboles publics de spark_streaming.py pour que
`from spark_streaming import start_streams` continue de fonctionner
exactement comme avant la réorganisation du dossier.
"""

from spark_streaming.spark_streaming import (
    SCHEMA,
    build_graph_dataframes,
    make_batch_processor,
    start_streams,
)

__all__ = [
    "SCHEMA",
    "build_graph_dataframes",
    "make_batch_processor",
    "start_streams",
]
