FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copie et installation des requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY . .

# Variables d'environnement par défaut
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV ELASTICSEARCH_HOST=elasticsearch:9200

# Script de démarrage
COPY start-alert-manager.sh /app/
RUN chmod +x /app/start-alert-manager.sh

# Santé du conteneur
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "print('Alert manager is running')" || exit 1

# Point d'entrée
CMD ["/app/start-alert-manager.sh"]