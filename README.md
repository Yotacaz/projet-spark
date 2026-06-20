# Projet Big Data — Streaming d'Interactions Commerciales (Style LeBonCoin)

> Cours électif : Spark et Big Data  
> CY Tech — ING1 Génie Informatique

## Architecture du pipeline

```
simulateur/          →   spark_streaming/    →   dashboard/
Producteur JSON           PySpark Structured       Visualisation
(source externe)          Streaming + GraphFrames  Graphe dynamique
```


## Démarrage rapide
Ce projet nécessite Python 3.11 et Java 17 à cause de Spark.
Commencez par installer les dépendances avec `uv` :
```bash
uv sync --upgrade
```

lancer l'application dans un seul terminal (pour linux):
```bash
bash start.sh
```

```bash
# uv (instalation des dépendances):
uv sync --upgrade
# activation de l'environnement virtuel(depuis la racine du projet):
source .venv/bin/activate

## DOCKER 
# 0. lancer docker desktop (ou utiliser sudo service docker restart sur linux)
# 1. dans un terminal dedié:
docker-compose up

# 2. Lancer le simulateur
python3 simulateur/simulateur.py

# 3. Lancer spark_streaming
python3 spark_streaming/spark_streaming.py


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


Pour spécifier la bonne version de Java (sous linux): 
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_LOCAL_IP=127.0.0.1
```

