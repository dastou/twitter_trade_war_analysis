#!/bin/bash

set -e  # Arrêter en cas d'erreur

echo "🚀 Setting up Sentiment Analysis Pipeline..."

# Fonction pour attendre qu'un service soit prêt
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=${4:-60}
    local attempt=1

    echo "📋 Waiting for $service_name..."
    while ! nc -z $host $port 2>/dev/null; do
        if [ $attempt -eq $max_attempts ]; then
            echo "❌ ERROR: $service_name not available after $max_attempts attempts"
            return 1
        fi
        echo "$service_name not ready, waiting... (attempt $attempt/$max_attempts)"
        sleep 5
        attempt=$((attempt + 1))
    done
    echo "✅ $service_name is ready!"
    return 0
}

# Attendre que les services de base soient prêts
wait_for_service localhost 9092 "Kafka" 60
wait_for_service localhost 9200 "Elasticsearch" 60

# Attendre que Kafka soit vraiment prêt pour les commandes
echo "📋 Waiting for Kafka to be fully ready..."
sleep 15

# Créer les topics Kafka avec gestion d'erreur
echo "📋 Creating Kafka topics..."

create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication=$3
    
    echo "Creating topic: $topic_name"
    if docker exec kafka kafka-topics --create \
        --topic $topic_name \
        --bootstrap-server localhost:9092 \
        --partitions $partitions \
        --replication-factor $replication \
        --if-not-exists 2>/dev/null; then
        echo "✅ Topic $topic_name created successfully"
    else
        echo "⚠️  Topic $topic_name might already exist or there was an error"
    fi
}

# Créer tous les topics
create_topic "raw-tweets" 3 1
create_topic "processed-tweets" 3 1
create_topic "anomalies" 1 1
create_topic "alerts" 1 1

# Vérifier que les topics ont été créés
echo "📋 Verifying topics..."
if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q "raw-tweets"; then
    echo "✅ Topics verified successfully"
else
    echo "⚠️  Warning: Some topics might not have been created properly"
fi

# Attendre qu'Elasticsearch soit prêt pour les requêtes
echo "⏳ Waiting for Elasticsearch to be ready for queries..."
max_attempts=30
attempt=1
while ! curl -s http://localhost:9200/_cluster/health | grep -q '"status":"green\|yellow"'; do
    if [ $attempt -eq $max_attempts ]; then
        echo "⚠️  WARNING: Elasticsearch cluster not healthy, continuing anyway..."
        break
    fi
    echo "Elasticsearch cluster not ready, waiting... (attempt $attempt/$max_attempts)"
    sleep 5
    attempt=$((attempt + 1))
done

# Appliquer les templates Elasticsearch
echo "📊 Applying Elasticsearch templates..."

apply_template() {
    local template_name=$1
    local template_file=$2
    
    if [ -f "$template_file" ]; then
        echo "Applying template: $template_name"
        if curl -s -X PUT "localhost:9200/_index_template/$template_name" \
            -H "Content-Type: application/json" \
            -d @"$template_file" > /dev/null; then
            echo "✅ Template $template_name applied successfully"
        else
            echo "⚠️  Warning: Failed to apply template $template_name"
        fi
    else
        echo "⚠️  Warning: Template file $template_file not found"
    fi
}

# Appliquer les templates
apply_template "tweets-template" "config/elasticsearch/index-templates/tweets-template.json"
apply_template "anomalies-template" "config/elasticsearch/index-templates/anomalies-template.json"

# Créer les index initiaux
echo "📊 Creating initial indices..."
curl -s -X PUT "localhost:9200/tweets-$(date +%Y-%m)" -H "Content-Type: application/json" \
    -d '{"settings": {"number_of_shards": 1, "number_of_replicas": 0}}' > /dev/null
curl -s -X PUT "localhost:9200/anomalies-$(date +%Y-%m)" -H "Content-Type: application/json" \
    -d '{"settings": {"number_of_shards": 1, "number_of_replicas": 0}}' > /dev/null

echo "✅ Setup completed successfully!"
echo ""
echo "🎯 Next steps:"
echo "1. Check that all services are running: docker-compose ps"
echo "2. Monitor logs: docker-compose logs -f"
echo "3. Access Grafana: http://localhost:3000 (admin/admin)"
echo "4. Access Kibana: http://localhost:5601"
echo "5. Check Elasticsearch: curl http://localhost:9200/_cluster/health"