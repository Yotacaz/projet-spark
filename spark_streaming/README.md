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

-------------------------------------------                                     
Batch: 1 (Activite par villes)
-------------------------------------------
+-------------------+----------------+----------+
|debut              |user_city       |nb_actions|
+-------------------+----------------+----------+
|2026-06-11 16:22:00|Bordeaux        |1         |
|2026-06-11 16:22:00|Strasbourg      |2         |
|2026-06-11 16:14:00|Rouen           |2         |
|2026-06-11 16:20:00|Pau             |4         |
|2026-06-11 16:18:00|Reims           |4         |
|2026-06-11 16:14:00|Metz            |2         |
|2026-06-11 16:22:00|Dijon           |1         |
|2026-06-11 16:14:00|Orléans         |3         |
|2026-06-11 16:16:00|Lyon            |8         |
|2026-06-11 16:18:00|Nîmes           |3         |
|2026-06-11 16:18:00|Orléans         |6         |
|2026-06-11 16:22:00|Nantes          |2         |
|2026-06-11 16:22:00|Caen            |2         |
|2026-06-11 16:20:00|La Rochelle     |4         |
|2026-06-11 16:20:00|Rennes          |5         |
|2026-06-11 16:14:00|Clermont-Ferrand|1         |
|2026-06-11 16:14:00|Mulhouse        |2         |
|2026-06-11 16:18:00|Perpignan       |3         |
|2026-06-11 16:14:00|Strasbourg      |1         |
|2026-06-11 16:16:00|Strasbourg      |2         |
+-------------------+----------------+----------+

-------------------------------------------                                     
Batch: 1 (agregation par actions)
-------------------------------------------
+-------------------+-------------------+-----------+---------+------------------+------------------+
|debut              |fin                |action_type|nb_events|chiffre_affaires  |prix_moyen        |
+-------------------+-------------------+-----------+---------+------------------+------------------+
|2026-06-11 16:25:00|2026-06-11 16:30:00|AIME       |36       |885722.9500000002 |24603.41527777778 |
|2026-06-11 16:20:00|2026-06-11 16:25:00|AIME       |41       |964728.4          |23529.960975609756|
|2026-06-11 16:20:00|2026-06-11 16:25:00|VOUT       |21       |516271.25999999995|24584.34571428571 |
|2026-06-11 16:25:00|2026-06-11 16:30:00|ACHAT      |3        |3474.64           |1158.2133333333334|
|2026-06-11 16:25:00|2026-06-11 16:30:00|VOUT       |7        |14901.66          |2128.808571428571 |
|2026-06-11 16:20:00|2026-06-11 16:25:00|ACHAT      |8        |11155.1           |1394.3875         |
+-------------------+-------------------+-----------+---------+------------------+------------------+

Ce message signifie simplement qu'entre le lancement du simulateur et le lancement de spark_streaming un retard c'est creuser(il sera consommer a terme):
26/06/11 18:44:07 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 10000 milliseconds, but spent 23449 milliseconds