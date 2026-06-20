# spark_streaming — Pipeline Kafka + Structured Streaming

Pipeline de traitement en temps réel d'événements marketplace, basé sur **PySpark Structured Streaming** et **Apache Kafka**. Construit les `vertices`/`edges` du graphe à chaque micro-batch et les transmet à `graph/graph.py` pour persistance (Delta Lake) et calcul d'indicateurs (GraphFrames).

## Vue d'ensemble

```
simulateur/ ──► Kafka (marketplace-events) ──► Spark Structured Streaming
                                                        │
                                          ┌─────────────┴──────────────┐
                                          │                            │
                                    q1 : agrégation              q2 : foreachBatch
                                    par action/minute            → main.handle_graph()
                                          │                            │
                                       console            graph.handle_new_data()
                                                          → Delta Lake (vertices/edges)
```

Les indicateurs GraphFrames (PageRank, composants connectés) ne sont **pas**
calculés à chaque micro-batch — c'est trop coûteux pour du temps réel. Ils
sont calculés à la demande sur les tables Delta via `GET /api/graph-metrics`
(voir `graph/graph.py` et `dashboard/`).

## Prérequis

- Python 3.11+
- Java 17 (JDK Temurin recommandé)
- Apache Kafka tournant sur `localhost:9092` (`docker-compose up` à la racine)

```bash
uv sync --upgrade
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows
```

Les dépendances Kafka, Delta Lake et GraphFrames sont téléchargées
automatiquement par Spark au premier lancement via `spark.jars.packages`
(défini dans `config.get_spark_session()`) :

```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
io.delta:delta-spark_2.12:3.2.0
graphframes:graphframes:0.8.3-spark3.0-s_2.12
```

## Structure des événements

Chaque message Kafka contient un JSON avec 8 champs :

| Champ         | Type   | Description                                     |
|---------------|--------|-------------------------------------------------|
| `timestamp`   | String | Horodatage ISO 8601 (`2026-05-25T09:15:30Z`)    |
| `user_id`     | String | Identifiant de l'utilisateur                    |
| `user_city`   | String | Ville de l'utilisateur                          |
| `product_id`  | String | Identifiant du produit                          |
| `product_cat` | String | Catégorie du produit                            |
| `seller_id`   | String | Identifiant du vendeur                          |
| `action_type` | String | Type d'action : `AIME`, `VOUT`, `ACHAT`         |
| `price`       | Double | Prix du produit                                 |

## Pipeline de traitement (`spark_streaming.py`)

**1. Lecture Kafka**
Spark consomme le topic `marketplace-events` en continu depuis les derniers offsets (`startingOffsets=latest`). Kafka expose chaque message dans une colonne `value` binaire.

**2. Désérialisation JSON**
La colonne `value` est castée en string puis parsée via `from_json()` selon le schéma strict `SCHEMA` (Schema Enforcement — pas d'inférence). Le champ `timestamp` est converti en `TimestampType`.

**3. Watermark**
Un watermark d'1 minute est appliqué sur `timestamp` (`withWatermark`) pour gérer les retards de messages et permettre le nettoyage automatique des anciens états en mémoire.

**4. Query q1 — Agrégation par action (fenêtre tumbling d'1 minute)**
Affichée sur la console toutes les 10 secondes en mode `update` :
- nombre d'événements par type d'action (`count("*")`)
- chiffre d'affaires total par type d'action (`sum("price")`)

**5. Query q2 — Construction du graphe (`foreachBatch`)**
À chaque micro-batch, `build_graph_dataframes()` :

- Construit les **vertices** (3 types de nœuds) :
  - `USER` — utilisateurs (`user_id`)
  - `PROD` — produits (`product_id`)
  - `SEL` — vendeurs (`seller_id`)
- Construit les **edges** : `user_id → product_id`, avec `relationship = action_type` et un `weight` égal au prix pour `VOUT`/`ACHAT`, nul pour `AIME`.
- Transmet `(vertices, edges, epoch_id)` au callback `on_graph` (par défaut `main.handle_graph`), qui délègue à `graph.handle_new_data()` pour le merge incrémental dans les tables Delta.

## Lancement

```bash
# Terminal 1 — Kafka + ZooKeeper (si pas déjà lancés)
docker-compose up        # première fois
docker-compose up -d     # si déjà créés, en arrière-plan

# Terminal 2 — simulateur → Kafka
python simulateur/simulateur.py

# Terminal 3 — pipeline Spark (lit Kafka, écrit Delta)
python main.py

# Terminal 4 — dashboard (API + interface)
python app.py
```

Ou via `bash start.sh` qui orchestre Docker + les trois processus Python.

## Configuration Windows / Linux

`config.py` détecte automatiquement la plateforme et configure les variables d'environnement nécessaires (`JAVA_HOME`, `HADOOP_HOME`, `SPARK_LOCAL_IP`). Ces valeurs peuvent être surchargées via les variables d'environnement système sans modifier le code.
