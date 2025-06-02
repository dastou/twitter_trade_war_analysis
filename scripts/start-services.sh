#!/bin/bash

set -e

echo "🚀 Starting Sentiment Analysis Pipeline..."

# Vérifier que Docker est en cours d'exécution
if ! docker info > /dev/null 2>&1; then
    echo "❌ ERROR: Docker is not running. Please start Docker Desktop."
    exit 1
fi

# Vérifier que docker-compose est disponible
if ! command -v docker-compose > /dev/null 2>&1; then
    echo "❌ ERROR: docker-compose not found. Please install Docker Compose."
    exit 1
fi

# Nettoyer les anciens conteneurs si nécessaire
echo "🧹 Cleaning up any existing containers..."
docker-compose down -v 2>/dev/null || true

# Démarrer les services de base en premier
echo "📋 Starting core infrastructure services..."
docker-compose up -d kafka elasticsearch grafana

# Attendre que les services de base soient prêts
echo "⏳ Waiting for core services to be ready..."
sleep 30

# Vérifier que Kafka est prêt
echo "🔍 Checking Kafka health..."
max_attempts=30
attempt=1
while ! docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    if [ $attempt -eq $max_attempts ]; then
        echo "❌ ERROR: Kafka not ready after $max_attempts attempts"
        exit 1
    fi
    echo "Kafka not ready, waiting... (attempt $attempt/$max_attempts)"
    sleep 10
    attempt=$((attempt + 1))
done
echo "✅ Kafka is ready"

# Vérifier qu'Elasticsearch est prêt
echo "🔍 Checking Elasticsearch health..."
max_attempts=30
attempt=1
while ! curl -s http://localhost:9200/_cluster/health | grep -q '"status":"green\|yellow"'; do
    if [ $attempt -eq $max_attempts ]; then
        echo "❌ ERROR: Elasticsearch not ready after $max_attempts attempts"
        exit 1
    fi
    echo "Elasticsearch not ready, waiting... (attempt $attempt/$max_attempts)"
    sleep 10
    attempt=$((attempt + 1))
done
echo "✅ Elasticsearch is ready"

# Exécuter le setup initial
echo "⚙️  Running initial setup..."
bash scripts/setup.sh

# Démarrer Spark
echo "📋 Starting Spark services..."
docker-compose up -d spark-master spark-worker

# Attendre que Spark soit prêt
echo "⏳ Waiting for Spark to be ready..."
sleep 20

# Démarrer les services d'application
echo "📋 Starting application services..."
docker-compose up -d tweet-simulator spark-processor alert-system

# Démarrer Kibana (optionnel)
echo "📋 Starting Kibana..."
docker-compose up -d kibana

# Vérifier le statut de tous les services
echo "🔍 Checking all services status..."
sleep 10
docker-compose ps

# Afficher les informations de connexion
echo ""
echo "🎉 Sentiment Analysis Pipeline started successfully!"
echo ""
echo "📊 Access Points:"
echo "  • Grafana Dashboard: http://localhost:3000 (admin/admin)"
echo "  • Kibana: http://localhost:5601"
echo "  • Elasticsearch: http://localhost:9200"
echo "  • Spark Master UI: http://localhost:8081"
echo "  • Spark Worker UI: http://localhost:8082"
echo ""
echo "📋 Useful Commands:"
echo "  • View logs: docker-compose logs -f [service_name]"
echo "  • Stop all: docker-compose down"
echo "  • Restart service: docker-compose restart [service_name]"
echo "  • Check status: docker-compose ps"
echo ""
echo "⚡ The system is now generating tweets and analyzing sentiment!"
echo "   Check Grafana dashboard for real-time monitoring."