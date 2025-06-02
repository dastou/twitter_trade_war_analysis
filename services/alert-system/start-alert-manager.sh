#!/bin/bash

echo "Starting Alert Manager..."

# Fonction pour attendre un service
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=60
    local attempt=1

    echo "Waiting for $service_name..."
    while ! nc -z $host $port; do
        if [ $attempt -eq $max_attempts ]; then
            echo "ERROR: $service_name not available after $max_attempts attempts"
            exit 1
        fi
        echo "$service_name not ready, waiting... (attempt $attempt/$max_attempts)"
        sleep 5
        attempt=$((attempt + 1))
    done
    echo "$service_name is ready!"
}

# Attendre que Kafka soit disponible
wait_for_service kafka 29092 "Kafka"

# Attendre qu'Elasticsearch soit disponible (optionnel)
echo "Checking Elasticsearch availability..."
if nc -z elasticsearch 9200; then
    echo "Elasticsearch is available"
    
    # Vérifier la santé d'Elasticsearch
    max_attempts=20
    attempt=1
    while ! curl -s http://elasticsearch:9200/_cluster/health > /dev/null; do
        if [ $attempt -eq $max_attempts ]; then
            echo "WARNING: Elasticsearch not responding after $max_attempts attempts, continuing anyway..."
            break
        fi
        echo "Elasticsearch not responding, waiting... (attempt $attempt/$max_attempts)"
        sleep 3
        attempt=$((attempt + 1))
    done
else
    echo "WARNING: Elasticsearch not available, continuing without it..."
fi

# Attendre que Grafana soit disponible (optionnel)
echo "Checking Grafana availability..."
if nc -z grafana 3000; then
    echo "Grafana is available"
    
    max_attempts=20
    attempt=1
    while ! curl -s http://grafana:3000/api/health > /dev/null; do
        if [ $attempt -eq $max_attempts ]; then
            echo "WARNING: Grafana not responding after $max_attempts attempts, continuing anyway..."
            break
        fi
        echo "Grafana not responding, waiting... (attempt $attempt/$max_attempts)"
        sleep 3
        attempt=$((attempt + 1))
    done
else
    echo "WARNING: Grafana not available, continuing without it..."
fi

# Démarrer le gestionnaire d'alertes
echo "Starting Alert Manager..."
exec python alert_manager.py