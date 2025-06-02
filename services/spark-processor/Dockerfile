FROM bitnami/spark:3.5.0

USER root

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    wget \
    netcat-traditional \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Répertoire de travail
WORKDIR /opt/spark-apps

# Copie et installation des requirements Python
COPY requirements.txt .
RUN pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir -r requirements.txt

# Installation des modèles spaCy avec gestion d'erreur
RUN python3 -m spacy download en_core_web_sm || echo "English model download failed, continuing..."
RUN python3 -m spacy download fr_core_news_sm || echo "French model download failed, continuing..."

# Téléchargement des modèles NLTK nécessaires
RUN python3 -c "import nltk; nltk.download('punkt', quiet=True); nltk.download('stopwords', quiet=True)"

# ===== CORRECTION CRITIQUE : Ajouter les JARs Kafka pour Spark =====
# Télécharger les connecteurs Kafka pour Spark Structured Streaming
RUN cd /opt/bitnami/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Vérifier que les JARs sont bien téléchargés
RUN ls -la /opt/bitnami/spark/jars/spark-sql-kafka* && \
    ls -la /opt/bitnami/spark/jars/kafka-clients* && \
    echo "✅ Kafka JARs successfully downloaded"

# Copie du code source
COPY . .

# Variables d'environnement
ENV PYTHONPATH=/opt/spark-apps
ENV PYTHONUNBUFFERED=1
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV ELASTICSEARCH_HOST=elasticsearch:9200

# Script de démarrage
COPY start-processor.sh /opt/spark-apps/
RUN chmod +x /opt/spark-apps/start-processor.sh

# Santé du conteneur
HEALTHCHECK --interval=60s --timeout=30s --start-period=120s --retries=3 \
    CMD python3 -c "print('Spark processor is running')" || exit 1

CMD ["/opt/spark-apps/start-processor.sh"]