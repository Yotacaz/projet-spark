#!/bin/bash

cleanup() {
    echo ""
    echo "Stopping application..."

    # Stop Python processes if they exist
    [ -n "$SIMULATEUR_PID" ] && kill -TERM "$SIMULATEUR_PID" 2>/dev/null || true
    [ -n "$APP_PID" ] && kill -TERM "$APP_PID" 2>/dev/null || true
    [ -n "$MAIN_PID" ] && kill -TERM "$MAIN_PID" 2>/dev/null || true

    # Wait for processes to terminate
    wait "$SIMULATEUR_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true
    wait "$MAIN_PID" 2>/dev/null || true

    # Uncomment if Ctrl+C should also stop Kafka and ZooKeeper
    # docker compose -f "$COMPOSE_FILE" down

    echo "Shutdown complete."
    exit 0

}

trap cleanup SIGINT SIGTERM

source .venv/bin/activate
uv sync --upgrade
set -e

COMPOSE_FILE="docker-compose.yml"

# Vérifie que Docker est lancé, sinon le démarre
if ! docker info >/dev/null 2>&1; then
    echo "Docker n'est pas démarré. Tentative de démarrage..."

    sudo systemctl start docker

    # Attente que Docker soit prêt
    for i in {1..20}; do
        if docker info >/dev/null 2>&1; then
            echo "Docker est maintenant démarré."
            break
        fi
        sleep 1
    done

    if ! docker info >/dev/null 2>&1; then
        echo "Erreur : impossible de démarrer Docker."
        exit 1
    fi
fi


# Vérifie que les services kafka et zookeeper sont démarrés
KAFKA_RUNNING=$(docker compose -f "$COMPOSE_FILE" ps -q kafka | xargs -r docker inspect -f '{{.State.Running}}' 2>/dev/null || echo "false")
ZOOKEEPER_RUNNING=$(docker compose -f "$COMPOSE_FILE" ps -q zookeeper | xargs -r docker inspect -f '{{.State.Running}}' 2>/dev/null || echo "false")

if [ "$KAFKA_RUNNING" = "true" ] && [ "$ZOOKEEPER_RUNNING" = "true" ]; then
    echo "Kafka et ZooKeeper sont déjà démarrés."
else
    echo "Kafka ou ZooKeeper n'est pas démarré. Lancement du docker-compose..."
    docker compose -f "$COMPOSE_FILE" up -d
    KAFKA_CONTAINER=$(docker compose -f "$COMPOSE_FILE" ps -q kafka)
    echo "Attente que Kafka soit disponible..."

    while true; do
        STATUS=$(docker inspect -f '{{.State.Health.Status}}' "$KAFKA_CONTAINER" 2>/dev/null)

        if [ "$STATUS" = "healthy" ]; then
            echo "Kafka est prêt."
            break
        fi

        echo "Statut Kafka : ${STATUS:-unknown}"
        sleep 2

    done

    docker compose -f "$COMPOSE_FILE" ps
    
fi

# Lancement des scripts Python
echo "Lancement des scripts Python..."
echo "lancement du main.py... (5 secondes d'attente après le lancement pour laisser le temps à spark de démarrer)"
python3 main.py &
MAIN_PID=$!

sleep 5

echo "lancement du simulateur/simulateur.py..."
python3 simulateur/simulateur.py
SIMULATEUR_PID=$!



wait