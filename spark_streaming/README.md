# Composant PySpark Structured Streaming

> 🔧 À implémenter

Ce dossier contiendra le script principal PySpark qui :
- Consomme le flux JSON du simulateur
- Applique le fenêtrage (Sliding/Tumbling Windows) et le watermarking
- Construit et met à jour le graphe (GraphFrames)
- Alimente le dashboard

## Concepts PySpark à implémenter

- `SparkSession` avec configuration mémoire/shuffle
- `readStream().format("json").schema(...)` — Schema Enforcement strict
- `withWatermark()` — gestion des retards
- Fenêtres glissantes / tronquantes (`window()`)
- `GraphFrames` — vertices (U, S, P) + edges (AIME, VOUT, ACHAT)
- Output modes : Append / Update / Complete
