#!/bin/bash

echo "Starting Spark Sentiment Processor..."

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

# Attendre qu'Elasticsearch soit disponible
wait_for_service elasticsearch 9200 "Elasticsearch"

# Vérifier la santé d'Elasticsearch
echo "Checking Elasticsearch health..."
max_attempts=30
attempt=1
while ! curl -s http://elasticsearch:9200/_cluster/health | grep -q '"status":"green\|yellow"'; do
    if [ $attempt -eq $max_attempts ]; then
        echo "WARNING: Elasticsearch not healthy after $max_attempts attempts, continuing anyway..."
        break
    fi
    echo "Elasticsearch not healthy, waiting... (attempt $attempt/$max_attempts)"
    sleep 5
    attempt=$((attempt + 1))
done

# Télécharger les modèles NLTK si nécessaire
echo "Ensuring NLTK data is available..."
python3 -c "
import nltk
try:
    nltk.data.find('tokenizers/punkt')
    print('NLTK punkt already available')
except LookupError:
    print('Downloading NLTK punkt...')
    nltk.download('punkt', quiet=True)

try:
    nltk.data.find('corpora/stopwords')
    print('NLTK stopwords already available')
except LookupError:
    print('Downloading NLTK stopwords...')
    nltk.download('stopwords', quiet=True)
"

# ===== CORRECTION SPARK PYTHONPATH =====
echo "Setting up Spark environment..."

# Définir SPARK_HOME
export SPARK_HOME=/opt/bitnami/spark

# Ajouter PySpark au PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Vérifier les chemins Spark
echo "SPARK_HOME: $SPARK_HOME"
echo "PYTHONPATH: $PYTHONPATH"

# Tester l'import PySpark
echo "Testing PySpark import..."
python3 -c "
try:
    from pyspark.sql import SparkSession
    print('✅ PySpark import successful!')
except ImportError as e:
    print('❌ PySpark import failed:', str(e))
    exit(1)
"

# Démarrer le processeur
echo "Starting sentiment processor..."
exec python3 sentiment_processor.py