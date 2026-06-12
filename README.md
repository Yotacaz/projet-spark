# Projet Big Data — Streaming d'Interactions Commerciales (Style LeBonCoin)

> Module : Architecture et Programmation Distribuée Big Data  
> CY Tech — ING2 Génie Informatique

## Architecture du pipeline

```
simulateur/          →   spark_streaming/    →   dashboard/
Producteur JSON           PySpark Structured       Visualisation
(source externe)          Streaming + GraphFrames  Graphe dynamique
```

## Composants

| Dossier | Rôle | État |
|---|---|---|
| `simulateur/` | Générateur de flux d'événements JSON | ✅ Terminé |
| `spark_streaming/` | Traitement du flux (Spark + GraphFrames) | 🔧 En cours |
| `dashboard/` | Interface graphique dynamique | 🔧 En cours |

## Démarrage rapide

```bash


# 1. Lancer le simulateur (stdout)
python simulateur/simulateur.py

# 2. Lancer en mode fichier (pour Spark)
python simulateur/simulateur.py --mode file --output-dir /data/stream
```

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

## Schéma PySpark (à utiliser dans `spark_streaming/`)

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
          .format("json") \
          .schema(event_schema) \
          .load("./data/stream/")
```
uv sync --upgrade
```

Pour spécifier la bonne version de Java (sous linux): 
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_LOCAL_IP=127.0.0.1
```

## DOCKER 
 lancer docker desktop
 dans un terminal dedier:

  > docker-compose up -d
