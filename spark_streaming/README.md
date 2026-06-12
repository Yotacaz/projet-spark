# Composant PySpark Structured Streaming

Ce dossier contiendra le script principal PySpark qui :
- Consomme le flux JSON du simulateur
- Applique le fenêtrage (Sliding/Tumbling Windows) et le watermarking
- Alimente le graphe (GraphFrames)
- Alimente le dashboard

## Concepts PySpark à implémenter

- `SparkSession` avec configuration mémoire/shuffle
- `readStream().format("json").schema(...)` — Schema Enforcement strict
- `withWatermark()` — gestion des retards
- Fenêtres glissantes / tronquantes (`window()`)
- `GraphFrames` — vertices (U, S, P) + edges (AIME, VOUT, ACHAT)
- Output modes : Append / Update / Complete

Important:
- J'ai modifier le simulateur
- Il faut adapter la tete du fichier pyspark

Sortie attendue:

