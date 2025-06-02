#!/bin/bash

set -e

echo "🧪 Testing Sentiment Analysis Pipeline..."

# Fonction pour tester un endpoint
test_endpoint() {
    local url=$1
    local service_name=$2
    local expected_status=${3:-200}
    
    echo "Testing $service_name ($url)..."
    
    if response=$(curl -s -w "%{http_code}" -o /tmp/response "$url"); then
        if [ "$response" = "$expected_status" ]; then
            echo "✅ $service_name is responding correctly"
            return 0
        else
            echo "⚠️  $service_name returned status $response (expected $expected_status)"
            return 1
        fi
    else
        echo "❌ $service_name is not responding"
        return 1
    fi
}

# Fonction pour tester Kafka
test_kafka() {
    echo "Testing Kafka..."
    if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q "raw-tweets"; then
        echo "✅ Kafka topics are available"
        return 0
    else
        echo "❌ Kafka topics not found"
        return 1
    fi
}

# Fonction pour tester Elasticsearch
test_elasticsearch() {
    echo "Testing Elasticsearch..."
    if curl -s "http://localhost:9200/_cluster/health" | grep -q '"status":"green\|yellow"'; then
        echo "✅ Elasticsearch cluster is healthy"
        
        # Vérifier les index
        if curl -s "http://localhost:9200/_cat/indices" | grep -q "tweets\|anomalies"; then
            echo "✅ Elasticsearch indices are present"
        else
            echo "⚠️  Elasticsearch indices not yet created (normal for new installation)"
        fi
        return 0
    else
        echo "❌ Elasticsearch cluster is not healthy"
        return 1
    fi
}

# Fonction pour tester les conteneurs
test_containers() {
    echo "Testing container status..."
    
    local services=("kafka" "elasticsearch" "grafana" "spark-master" "spark-worker" "tweet-simulator" "spark-processor" "alert-system")
    local failed_services=()
    
    for service in "${services[@]}"; do
        if docker-compose ps "$service" | grep -q "Up"; then
            echo "✅ $service is running"
        else
            echo "❌ $service is not running"
            failed_services+=("$service")
        fi
    done
    
    if [ ${#failed_services[@]} -eq 0 ]; then
        echo "✅ All services are running"
        return 0
    else
        echo "❌ Failed services: ${failed_services[*]}"
        return 1
    fi
}

# Exécuter tous les tests
echo "Starting pipeline tests..."
echo "=========================="

test_results=()

# Test des conteneurs
if test_containers; then
    test_results+=("Containers: ✅")
else
    test_results+=("Containers: ❌")
fi

# Test de Kafka
if test_kafka; then
    test_results+=("Kafka: ✅")
else
    test_results+=("Kafka: ❌")
fi

# Test d'Elasticsearch
if test_elasticsearch; then
    test_results+=("Elasticsearch: ✅")
else
    test_results+=("Elasticsearch: ❌")
fi

# Test des endpoints web
if test_endpoint "http://localhost:3000/api/health" "Grafana"; then
    test_results+=("Grafana: ✅")
else
    test_results+=("Grafana: ❌")
fi

if test_endpoint "http://localhost:5601/api/status" "Kibana"; then
    test_results+=("Kibana: ✅")
else
    test_results+=("Kibana: ❌")
fi

if test_endpoint "http://localhost:8081" "Spark Master"; then
    test_results+=("Spark Master: ✅")
else
    test_results+=("Spark Master: ❌")
fi

if test_endpoint "http://localhost:8082" "Spark Worker"; then
    test_results+=("Spark Worker: ✅")
else
    test_results+=("Spark Worker: ❌")
fi

# Afficher le résumé
echo ""
echo "🏁 Test Results Summary:"
echo "========================"
for result in "${test_results[@]}"; do
    echo "  $result"
done

# Vérifier si tous les tests ont réussi
failed_count=$(printf '%s\n' "${test_results[@]}" | grep -c "❌" || true)
if [ "$failed_count" -eq 0 ]; then
    echo ""
    echo "🎉 All tests passed! Pipeline is working correctly."
    exit 0
else
    echo ""
    echo "⚠️  $failed_count test(s) failed. Check the logs for more details:"
    echo "   docker-compose logs -f"
    exit 1
fi