# Projet Big Data — Streaming d'Interactions Commerciales (Style LeBonCoin)

> Cours électif : Spark et Big Data
> CY Tech — ING1 Génie Informatique

## Architecture du pipeline

```
simulateur/  →  Kafka  →  spark_streaming/  →  graph/ (Delta + GraphFrames)  →  dashboard/
Producteur      topic     PySpark Structured    Stockage incrémental +           Visualisation
JSON             "marketplace-events"  Streaming      indicateurs de graphe            Graphe dynamique
```

## Composants

| Dossier | Rôle | État |
|---|---|---|
| `simulateur/` | Générateur de flux d'événements JSON → Kafka | ✅ Terminé |
| `spark_streaming/` | Lecture Kafka, windowing, watermarking, construction vertices/edges | ✅ Terminé |
| `graph/` | Persistance Delta Lake + indicateurs GraphFrames (PageRank, composants connectés) | ✅ Terminé |
| `app.py` | API Flask servant le dashboard et les requêtes de graphe | ✅ Terminé |
| `dashboard/` | Interface graphique dynamique (vis-network) | ✅ Terminé |

## Démarrage rapide

Ce projet nécessite Python 3.11 et Java 17 à cause de Spark.

### 1. Installer les dépendances

```bash
uv sync --upgrade
source .venv/bin/activate
```

### 2. Variables d'environnement (Linux)

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_LOCAL_IP=127.0.0.1
```

### 3. Lancer Kafka + ZooKeeper

```bash
# 0. Lancer Docker Desktop (ou : sudo service docker restart sur Linux)
docker-compose up
```

### 4. Lancer les trois processus applicatifs

```bash
# Terminal A — le simulateur (producteur Kafka)
python3 simulateur/simulateur.py

# Terminal B — le pipeline Spark (consommateur Kafka, graphe Delta)
python3 main.py

# Terminal C — le dashboard (API Flask + interface)
python3 app.py
```

Ou en une commande, via le script fourni :

```bash
bash start.sh
```

Le dashboard est ensuite accessible sur **http://localhost:5000**.

## Schéma des événements

```json
{
  "timestamp":   "2026-05-25T09:15:30Z",
  "user_id":     "usr_9482",
  "user_city":   "Paris",
  "product_id":  "prod_5501",
  "product_cat": "Véhicules",
  "seller_id":   "sel_0214",
  "action_type": "VOUT",
  "price":       450.00
}
```

## Schéma PySpark (utilisé dans `spark_streaming/spark_streaming.py`)

```python
from pyspark.sql.types import *

event_schema = StructType([
    StructField("timestamp",   StringType(),  False),
    StructField("user_id",     StringType(),  False),
    StructField("user_city",   StringType(),  False),
    StructField("product_id",  StringType(),  False),
    StructField("product_cat", StringType(),  False),
    StructField("seller_id",   StringType(),  False),
    StructField("action_type", StringType(),  False),
    StructField("price",       DoubleType(),  False),
])

df = spark.readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("subscribe", "marketplace-events") \
          .load()
```

## Indicateurs de graphe (GraphFrames)

Le module `graph/graph.py` expose deux indicateurs calculés à la demande via
`GET /api/graph-metrics` :

- **PageRank** (`compute_pagerank`) — centralité de chaque nœud (utilisateur,
  produit, vendeur) dans le réseau d'interactions.
- **Composants connectés** (`compute_connected_components`) — regroupe les
  nœuds en clusters indépendants.

Ces calculs sont coûteux (itératifs, distribués) : ils ne sont **pas**
inclus dans la boucle d'auto-refresh du dashboard, mais déclenchés
ponctuellement via le bouton "Compute Metrics" de la sidebar.

> Validation locale : `python3 test_graphframes.py` (nécessite un accès
> réseau complet pour résoudre le jar Maven `graphframes:graphframes:0.8.3-spark3.0-s_2.12`).
