# spark_streaming — Pipeline Kafka + GraphFrames

Pipeline de traitement en temps réel d'événements marketplace, basé sur **PySpark Structured Streaming**, **Apache Kafka** et **GraphFrames**.

## Vue d'ensemble

```
simulateur/ ──► Kafka (marketplace-events) ──► Spark Structured Streaming
                                                        │
                                          ┌─────────────┴──────────────┐
                                          │                            │
                                    q1 : agrégation              q2 : GraphFrames
                                    par action/minute            top produits
                                          │                            │
                                       console               Kafka (graph-vertices
                                                                  graph-edges)
```

## Prérequis

- Python 3.11+
- Java 17 (JDK Temurin recommandé)
- Apache Kafka tournant sur `localhost:9092`
- `winutils.exe` + `hadoop.dll` dans `C:\hadoop\bin\` (Windows uniquement)

```bash
uv venv
.venv\Scripts\activate   # Windows
uv pip install pyspark
```

Les dépendances Kafka et GraphFrames sont téléchargées automatiquement par Spark au premier lancement via `spark.jars.packages` :

```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
graphframes:graphframes:0.8.3-spark3.5-s_2.12
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

## Pipeline de traitement

**1. Lecture Kafka**
Spark consomme le topic `marketplace-events` en continu depuis les derniers offsets (`startingOffsets=latest`). Kafka expose chaque message dans une colonne `value` binaire.

**2. Désérialisation JSON**
La colonne `value` est castée en string puis parsée via `from_json()` selon le schéma défini. Le champ `timestamp` est converti en `TimestampType`.

**3. Watermark**
Un watermark d'1 minute est appliqué sur `timestamp` pour gérer les retards de messages et permettre les agrégations par fenêtre temporelle.

**4. Query q1 — Agrégation par action (fenêtre tumbling d'1 minute)**
Affichée sur la console toutes les 10 secondes en mode `update` :
- nombre d'événements par type d'action
- chiffre d'affaires total par type d'action

**5. Query q2 — Analyse de graphe avec GraphFrames (`foreachBatch`)**
À chaque micro-batch, la fonction `process_batch` :

- Construit un **graphe** avec trois types de nœuds (vertices) :
  - `USER` — utilisateurs
  - `PROD` — produits
  - `SEL` — vendeurs
- Construit les **arêtes** (edges) : `user_id → product_id` avec le type d'action comme relation
- Calcule et affiche le **top 5 des produits les plus populaires** via `inDegrees`
- Publie les vertices et edges dans deux topics Kafka de sortie :
  - `graph-vertices`
  - `graph-edges`

## Topics Kafka

| Topic                | Direction | Contenu                        |
|----------------------|-----------|--------------------------------|
| `marketplace-events` | entrée    | Événements bruts du simulateur |
| `graph-vertices`     | sortie    | Nœuds du graphe (JSON)         |
| `graph-edges`        | sortie    | Arêtes du graphe (JSON)        |

## Lancement

Lancer docker_desktop

```bash
# Terminal 1 — Kafka (si pas déjà lancé)
docker-compose up

docker-compose up -d (si il n'est pas creer)

# Terminal 2 — simulateur → Kafka
python simulateur/simulateur.py

# Terminal 3 — pipeline Spark
python spark_streaming/spark_streaming.py
```

## Configuration Windows

Le script détecte automatiquement Windows et configure les variables d'environnement nécessaires :

```python
if sys.platform == "win32":
    os.environ["JAVA_HOME"]   = "C:/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
    os.environ["HADOOP_HOME"] = "C:/hadoop"
```

Ces valeurs peuvent être surchargées via les variables d'environnement système sans modifier le code.

