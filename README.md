# Analyse en Temps Réel des Sentiments sur Twitter face aux Tensions Commerciales Internationales

## 📋 Aperçu du Projet

Ce projet implémente un système complet d'analyse de sentiment en temps réel pour surveiller les réactions aux tensions commerciales internationales. En raison des limitations d'accès à l'API Twitter payante, nous utilisons un simulateur de tweets sophistiqué qui génère des données réalistes.

**Technologies:** Kafka, Spark Streaming, Elasticsearch, Grafana, NLP, Docker

## 🏗️ Architecture

```
Simulateur de Tweets → Kafka → Spark Streaming → Elasticsearch → Grafana
                                   ↓
                         Détection d'Anomalies → Système d'Alertes → Notifications Email
```

## 🚀 Principales Fonctionnalités

- **Génération de tweets simulés** avec différents scénarios (normal, guerre commerciale, accords)
- **Analyse de sentiment en temps réel** utilisant VADER et un lexique commercial spécialisé
- **Détection d'anomalies multi-dimensionnelle** (volume, sentiment, engagement, mots-clés)
- **Dashboards Grafana interactifs** visualisant les tendances et distributions
- **Système d'alertes** avec notifications par email via MailHog
- **Distribution géographique** des tweets avec analyse par pays

## 🔧 Configuration et Installation

### Prérequis
- Docker et Docker Compose
- Git
- 8 GB RAM minimum recommandé

### Installation

1. Cloner le dépôt
```bash
git clone https://github.com/votre-username/sentiment-analysis-pipeline.git
cd sentiment-analysis-pipeline
```

2. Démarrer les services
```bash
docker-compose up -d
```

3. Créer les topics Kafka
```bash
docker exec kafka kafka-topics --create --topic raw-tweets --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic processed-tweets --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic anomalies --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic alerts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
```

4. Configurer les templates Elasticsearch
```bash
curl -X PUT "localhost:9200/_index_template/tweets-template" -H "Content-Type: application/json" -d @config/elasticsearch/index-templates/tweets-template.json
curl -X PUT "localhost:9200/_index_template/anomalies-template" -H "Content-Type: application/json" -d @config/elasticsearch/index-templates/anomalies-template.json
curl -X PUT "localhost:9200/tweets-$(date +%Y-%m)" -H "Content-Type: application/json"
curl -X PUT "localhost:9200/anomalies-$(date +%Y-%m)" -H "Content-Type: application/json"
```

## 📊 Accès aux Interfaces

- **Grafana**: http://localhost:3000 (admin/admin)
- **Kibana**: http://localhost:5601
- **Elasticsearch**: http://localhost:9200
- **MailHog** (serveur email de test): http://localhost:8025
- **Spark Master UI**: http://localhost:8081
- **Spark Worker UI**: http://localhost:8082

## 📁 Structure du Projet

```
sentiment-analysis-pipeline/
├── docker-compose.yml         # Configuration des services Docker
├── .env                       # Variables d'environnement
├── config/                    # Fichiers de configuration
│   ├── elasticsearch/         # Templates d'index Elasticsearch
│   └── grafana/               # Dashboards provisionnés
├── docker/                    # Fichiers Docker spécifiques
│   ├── kafka/
│   ├── elasticsearch/
│   └── spark/
├── services/                  # Services applicatifs
│   ├── tweet-simulator/       # Simulateur de tweets
│   │   ├── app.py            # Point d'entrée
│   │   ├── tweet_generator.py # Générateur de tweets
│   │   ├── data/             # Données pour la simulation
│   │   └── Dockerfile
│   ├── spark-processor/       # Traitement Spark
│   │   ├── sentiment_processor.py # Processeur principal
│   │   ├── nlp_analyzer.py    # Analyse NLP
│   │   ├── anomaly_detector.py # Détection d'anomalies
│   │   └── Dockerfile
│   └── alert-system/          # Système d'alertes
│       ├── alert_manager.py   # Gestionnaire d'alertes
│       └── Dockerfile
└── scripts/                   # Scripts utilitaires
    ├── setup.sh               # Configuration initiale
    ├── start-services.sh      # Démarrage des services
    └── test-pipeline.sh       # Tests du pipeline
```

## 💡 Utilisation

### Modification des Scénarios de Simulation

Pour changer le mode de simulation:
```bash
# Éditer le fichier .env
# Changer SIMULATION_MODE=trade_war_escalation à un autre mode
# Options: normal, trade_war_escalation, breakthrough_deal, supply_chain_crisis, market_uncertainty
docker-compose restart tweet-simulator
```

### Surveillance des Logs

```bash
# Tous les services
docker-compose logs -f

# Service spécifique
docker-compose logs -f tweet-simulator
docker-compose logs -f spark-processor
docker-compose logs -f alert-system
```

### Création de Dashboards Grafana

1. Accédez à Grafana (http://localhost:3000)
2. Créez un nouveau dashboard
3. Ajoutez un panneau avec source de données "Elasticsearch-Tweets"
4. Utilisez des groupements comme `sentiment_label.keyword` ou `original_tweet.location.country.keyword`

## 📊 Résultats Principaux

- **Distribution du sentiment**: 65% négatif, 20% positif, 15% neutre
- **Distribution géographique**: Dominance de la Chine et des États-Unis
- **Entreprises les plus mentionnées**: GE, Tesla, TSMC en tête
- **Alertes générées**: Détection de pics de sentiment négatif et volume anormal

## 🛠️ Limitations et Évolutions Futures

### Limitations actuelles
- Utilisation de données simulées plutôt que l'API Twitter réelle
- Analyse de sentiment basique (VADER)
- Performance limitée en environnement local

### Évolutions futures
- Intégration de modèles plus avancés (Transformers, HuggingFace)
- Amélioration de l'analyse multilingue
- Migration vers une architecture cloud native
- Intégration d'autres sources de données

---

Projet développé dans le cadre d'un cours sur le Big Data et l'analyse en temps réel.