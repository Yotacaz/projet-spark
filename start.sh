#!/bin/bash

set -e

COMPOSE_FILE="docker-compose.yml"

# Vérifie que les services kafka et zookeeper sont démarrés
KAFKA_RUNNING=$(docker compose -f "$COMPOSE_FILE" ps -q kafka | xargs -r docker inspect -f '{{.State.Running}}' 2>/dev/null || echo "false")
ZOOKEEPER_RUNNING=$(docker compose -f "$COMPOSE_FILE" ps -q zookeeper | xargs -r docker inspect -f '{{.State.Running}}' 2>/dev/null || echo "false")

if [ "$KAFKA_RUNNING" = "true" ] && [ "$ZOOKEEPER_RUNNING" = "true" ]; then
    echo "Kafka et ZooKeeper sont déjà démarrés."
else
    echo "Kafka ou ZooKeeper n'est pas démarré. Lancement du docker-compose..."
    docker compose -f "$COMPOSE_FILE" up -d

    echo "Attente du démarrage..."
    sleep 10

    docker compose -f "$COMPOSE_FILE" ps
    
fi

python3 simulateur/simulateur.py &
python3 app.py &
python3 main.py &