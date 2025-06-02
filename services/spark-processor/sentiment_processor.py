import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import sys

# Imports PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery

# Imports pour Kafka et Elasticsearch
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Imports de nos modules d'analyse
from nlp_analyzer import AdvancedNLPAnalyzer
from anomaly_detector import TradeAnomalyDetector

class SentimentStreamProcessor:
    def __init__(self):
        self.setup_logging()
        self.load_config()
        self.setup_spark_session()
        self.setup_external_connections()
        self.setup_analyzers()
        self.setup_schemas()
        
        # Compteurs de performance
        self.processed_tweets = 0
        self.detected_anomalies = 0
        self.alerts_sent = 0
        
    def setup_logging(self):
        """Configure le logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)

    def load_config(self):
        """Charge la configuration depuis les variables d'environnement"""
        self.config = {
            # Kafka
            'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'kafka_input_topic': os.getenv('KAFKA_TOPIC_TWEETS', 'raw-tweets'),
            'kafka_output_topic': os.getenv('KAFKA_TOPIC_PROCESSED', 'processed-tweets'),
            'kafka_anomaly_topic': os.getenv('KAFKA_TOPIC_ANOMALIES', 'anomalies'),
            'kafka_alert_topic': os.getenv('KAFKA_TOPIC_ALERTS', 'alerts'),
            
            # Elasticsearch
            'elasticsearch_host': os.getenv('ELASTICSEARCH_HOST', 'elasticsearch:9200'),
            'elasticsearch_index_tweets': os.getenv('ELASTICSEARCH_INDEX_TWEETS', 'tweets'),
            'elasticsearch_index_anomalies': os.getenv('ELASTICSEARCH_INDEX_ANOMALIES', 'anomalies'),
            
            # Spark
            'spark_master': os.getenv('SPARK_MASTER_URL', 'local[*]'),  # Fallback vers local si pas de master
            'batch_duration': int(os.getenv('SPARK_BATCH_DURATION', '5')),  # 5 secondes
            'checkpoint_location': os.getenv('SPARK_CHECKPOINT_LOCATION', '/tmp/spark-checkpoint'),
            
            # Processing
            'max_batch_size': int(os.getenv('MAX_BATCH_SIZE', '1000')),
            'enable_anomaly_detection': os.getenv('ENABLE_ANOMALY_DETECTION', 'true').lower() == 'true',
            'enable_alerts': os.getenv('ENABLE_ALERTS', 'true').lower() == 'true'
        }
        
        self.logger.info(f"Configuration loaded: {self.config}")

    def setup_spark_session(self):
        """Configure la session Spark"""
        try:
            # Configuration Spark optimisée et sécurisée
            spark_builder = SparkSession.builder \
                .appName("SentimentAnalysisProcessor") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.sql.streaming.checkpointLocation", self.config['checkpoint_location']) \
                .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
            
            # Essayer de se connecter au master Spark, sinon utiliser local
            try:
                if self.config['spark_master'] != 'local[*]':
                    spark_builder = spark_builder.master(self.config['spark_master'])
                    self.spark = spark_builder.getOrCreate()
                    self.logger.info(f"Connected to Spark master: {self.config['spark_master']}")
                else:
                    raise Exception("Using local mode")
            except Exception as e:
                self.logger.warning(f"Cannot connect to Spark master, using local mode: {e}")
                self.spark = spark_builder.master("local[*]").getOrCreate()
            
            # Réduire le niveau de log de Spark
            self.spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info("Spark session created successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {e}")
            raise

    def setup_external_connections(self):
        """Configure les connexions externes"""
        try:
            # Kafka Producer pour les résultats
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.config['kafka_bootstrap_servers'].split(','),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=1000,
                compression_type='gzip',
                batch_size=16384,
                linger_ms=100
            )
            
            # Client Elasticsearch
            self.elasticsearch_client = Elasticsearch(
                [f"http://{self.config['elasticsearch_host']}"],
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # Test de connexion
            if not self.elasticsearch_client.ping():
                self.logger.warning("Cannot connect to Elasticsearch, continuing without it")
                self.elasticsearch_client = None
                
            self.logger.info("External connections established")
            
        except Exception as e:
            self.logger.error(f"Failed to setup external connections: {e}")
            raise

    def setup_analyzers(self):
        """Initialise les analyseurs NLP et détection d'anomalies"""
        try:
            self.nlp_analyzer = AdvancedNLPAnalyzer()
            
            if self.config['enable_anomaly_detection']:
                self.anomaly_detector = TradeAnomalyDetector()
            else:
                self.anomaly_detector = None
                
            self.logger.info("Analyzers initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize analyzers: {e}")
            raise

    def setup_schemas(self):
        """Définit les schémas Spark SQL"""
        # Schéma pour les tweets entrants (plus permissif)
        self.input_schema = StructType([
            StructField("tweet_id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("user", StringType(), True),
            StructField("language", StringType(), True),
            StructField("sentiment", StructType([
                StructField("label", StringType(), True),
                StructField("score", DoubleType(), True),
                StructField("confidence", DoubleType(), True)
            ]), True),
            StructField("engagement", StructType([
                StructField("likes", IntegerType(), True),
                StructField("retweets", IntegerType(), True),
                StructField("replies", IntegerType(), True)
            ]), True),
            StructField("location", StructType([
                StructField("country", StringType(), True),
                StructField("region", StringType(), True),
                StructField("coordinates", ArrayType(DoubleType()), True)
            ]), True),
            StructField("keywords", ArrayType(StringType()), True),
            StructField("entities", StructType([
                StructField("companies", ArrayType(StringType()), True),
                StructField("countries", ArrayType(StringType()), True),
                StructField("persons", ArrayType(StringType()), True)
            ]), True),
            StructField("is_anomaly", BooleanType(), True),
            StructField("scenario", StringType(), True),
            StructField("priority", StringType(), True),
            StructField("producer_timestamp", StringType(), True),
            StructField("producer_id", StringType(), True),
            StructField("version", StringType(), True)
        ])

    def create_kafka_stream(self):
        """Crée le stream Kafka d'entrée"""
        try:
            kafka_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config['kafka_bootstrap_servers']) \
                .option("subscribe", self.config['kafka_input_topic']) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.session.timeout.ms", "30000") \
                .option("kafka.request.timeout.ms", "40000") \
                .option("maxOffsetsPerTrigger", "1000") \
                .load()
            
            # Décoder les messages JSON avec gestion d'erreur
            tweets_stream = kafka_stream.select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("message_value"),
                col("timestamp").alias("kafka_timestamp"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset")
            )
            
            # Parser JSON de manière sécurisée
            parsed_stream = tweets_stream.select(
                col("message_key"),
                col("kafka_timestamp"),
                col("kafka_partition"),
                col("kafka_offset"),
                from_json(col("message_value"), self.input_schema).alias("tweet")
            ).select(
                col("tweet.*"),
                col("message_key"),
                col("kafka_timestamp"),
                col("kafka_partition"),
                col("kafka_offset")
            ).filter(col("tweet_id").isNotNull())  # Filtrer les messages malformés
            
            self.logger.info("Kafka stream created successfully")
            return parsed_stream
            
        except Exception as e:
            self.logger.error(f"Failed to create Kafka stream: {e}")
            raise

    def process_tweet_batch(self, df, epoch_id):
        """Traite un batch de tweets"""
        try:
            start_time = datetime.now()
            batch_size = df.count()
            
            if batch_size == 0:
                return
                
            # Limiter la taille du batch pour éviter les problèmes de mémoire
            if batch_size > self.config['max_batch_size']:
                self.logger.warning(f"Batch size {batch_size} exceeds limit, sampling")
                df = df.sample(fraction=self.config['max_batch_size']/batch_size, seed=42)
                batch_size = df.count()
                
            self.logger.info(f"Processing batch {epoch_id} with {batch_size} tweets")
            
            # Convertir en pandas pour traitement avec gestion d'erreur
            try:
                tweets_pandas = df.toPandas()
            except Exception as e:
                self.logger.error(f"Error converting to pandas: {e}")
                return
            
            processed_tweets = []
            anomalies = []
            alerts = []
            
            for idx, row in tweets_pandas.iterrows():
                try:
                    # Préparer les données du tweet
                    tweet_data = self.prepare_tweet_data(row)
                    
                    if not tweet_data or not tweet_data.get('text'):
                        continue
                    
                    # Analyse NLP
                    nlp_result = self.analyze_tweet_sentiment(tweet_data)
                    
                    # Détection d'anomalies
                    anomaly_result = None
                    if self.config['enable_anomaly_detection'] and self.anomaly_detector:
                        anomaly_result = self.detect_tweet_anomaly(tweet_data, nlp_result)
                    
                    # Combiner les résultats
                    processed_tweet = self.combine_results(tweet_data, nlp_result, anomaly_result)
                    processed_tweets.append(processed_tweet)
                    
                    # Gestion des anomalies
                    if anomaly_result and anomaly_result.get('alert_required', False):
                        anomalies.append(anomaly_result)
                        
                        if self.config['enable_alerts']:
                            alert = self.create_alert(processed_tweet, anomaly_result)
                            alerts.append(alert)
                    
                    self.processed_tweets += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing individual tweet {idx}: {e}")
                    continue
            
            # Sauvegarder les résultats
            if processed_tweets:
                self.save_processed_tweets(processed_tweets)
                self.send_to_kafka(processed_tweets, self.config['kafka_output_topic'])
            
            if anomalies:
                self.save_anomalies(anomalies)
                self.send_to_kafka(anomalies, self.config['kafka_anomaly_topic'])
                self.detected_anomalies += len(anomalies)
            
            if alerts:
                self.send_to_kafka(alerts, self.config['kafka_alert_topic'])
                self.alerts_sent += len(alerts)
            
            # Statistiques
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"Batch {epoch_id} processed: {batch_size} tweets, "
                f"{len(anomalies)} anomalies, {len(alerts)} alerts "
                f"in {processing_time:.2f}s"
            )
            
        except Exception as e:
            self.logger.error(f"Error processing batch {epoch_id}: {e}")

    def prepare_tweet_data(self, row) -> Dict:
        """Prépare les données du tweet pour l'analyse"""
        try:
            # Fonction helper pour gérer les valeurs nulles
            def safe_get(obj, key, default=None):
                try:
                    value = getattr(obj, key, default) if hasattr(obj, key) else default
                    return value if value is not None else default
                except:
                    return default
            
            tweet_data = {
                'tweet_id': safe_get(row, 'tweet_id', ''),
                'text': safe_get(row, 'text', ''),
                'timestamp': safe_get(row, 'timestamp', datetime.now().isoformat()),
                'user': safe_get(row, 'user', ''),
                'language': safe_get(row, 'language', 'en'),
                'keywords': [],
                'entities': {},
                'engagement': {},
                'location': {},
                'is_anomaly': safe_get(row, 'is_anomaly', False),
                'scenario': safe_get(row, 'scenario', 'normal'),
                'priority': safe_get(row, 'priority', 'normal')
            }
            
            # Traitement sécurisé des objets complexes
            try:
                keywords_raw = safe_get(row, 'keywords', [])
                if keywords_raw and isinstance(keywords_raw, list):
                    tweet_data['keywords'] = [str(k) for k in keywords_raw if k is not None]
            except:
                tweet_data['keywords'] = []
            
            try:
                entities_raw = safe_get(row, 'entities')
                if entities_raw and hasattr(entities_raw, 'asDict'):
                    tweet_data['entities'] = entities_raw.asDict()
                elif isinstance(entities_raw, dict):
                    tweet_data['entities'] = entities_raw
            except:
                tweet_data['entities'] = {}
            
            try:
                engagement_raw = safe_get(row, 'engagement')
                if engagement_raw and hasattr(engagement_raw, 'asDict'):
                    tweet_data['engagement'] = engagement_raw.asDict()
                elif isinstance(engagement_raw, dict):
                    tweet_data['engagement'] = engagement_raw
            except:
                tweet_data['engagement'] = {}
            
            try:
                location_raw = safe_get(row, 'location')
                if location_raw and hasattr(location_raw, 'asDict'):
                    tweet_data['location'] = location_raw.asDict()
                elif isinstance(location_raw, dict):
                    tweet_data['location'] = location_raw
            except:
                tweet_data['location'] = {}
            
            return tweet_data
            
        except Exception as e:
            self.logger.error(f"Error preparing tweet data: {e}")
            return {}

    def analyze_tweet_sentiment(self, tweet_data: Dict) -> Dict:
        """Analyse le sentiment du tweet"""
        try:
            text = tweet_data.get('text', '')
            language = tweet_data.get('language', 'en')
            
            if not text or not text.strip():
                return {'error': 'Empty text'}
            
            # Analyse complète avec notre NLP analyzer
            analysis_result = self.nlp_analyzer.analyze_text(text, language)
            
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"Error in sentiment analysis: {e}")
            return {'error': str(e)}

    def detect_tweet_anomaly(self, tweet_data: Dict, nlp_result: Dict) -> Optional[Dict]:
        """Détecte les anomalies dans le tweet"""
        try:
            if not self.anomaly_detector:
                return None
            
            # Enrichir les données du tweet avec les résultats NLP
            enriched_tweet = tweet_data.copy()
            
            # Ajouter les résultats de l'analyse de sentiment
            if 'sentiment' in nlp_result:
                enriched_tweet['sentiment'] = nlp_result['sentiment']
            
            # Ajouter les features extraites
            if 'features' in nlp_result:
                enriched_tweet['features'] = nlp_result['features']
            
            # Détecter les anomalies
            anomaly_result = self.anomaly_detector.detect_anomalies(enriched_tweet)
            
            return anomaly_result
            
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return None

    def combine_results(self, tweet_data: Dict, nlp_result: Dict, anomaly_result: Optional[Dict]) -> Dict:
        """Combine tous les résultats d'analyse"""
        combined = {
            'processing_timestamp': datetime.now().isoformat(),
            'original_tweet': tweet_data,
            'nlp_analysis': nlp_result,
            'anomaly_detection': anomaly_result,
            'processing_info': {
                'processor_version': '1.0',
                'processing_node': 'spark-processor'
            }
        }
        
        # Ajouter les champs indexables pour Elasticsearch
        try:
            if 'sentiment' in nlp_result:
                combined['sentiment_label'] = nlp_result['sentiment'].get('label', 'neutral')
                combined['sentiment_score'] = nlp_result['sentiment'].get('score', 0.0)
                combined['sentiment_confidence'] = nlp_result['sentiment'].get('confidence', 0.0)
            else:
                combined['sentiment_label'] = 'neutral'
                combined['sentiment_score'] = 0.0
                combined['sentiment_confidence'] = 0.0
            
            if anomaly_result:
                combined['anomaly_score'] = anomaly_result.get('composite_score', 0.0)
                combined['anomaly_severity'] = anomaly_result.get('severity', 'low')
                combined['is_anomaly'] = anomaly_result.get('alert_required', False)
            else:
                combined['anomaly_score'] = 0.0
                combined['anomaly_severity'] = 'low'
                combined['is_anomaly'] = False
        except Exception as e:
            self.logger.error(f"Error combining results: {e}")
        
        return combined

    def create_alert(self, processed_tweet: Dict, anomaly_result: Dict) -> Dict:
        """Crée une alerte basée sur l'anomalie détectée"""
        try:
            alert = {
                'alert_id': f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{processed_tweet['original_tweet']['tweet_id']}",
                'timestamp': datetime.now().isoformat(),
                'type': 'trade_sentiment_anomaly',
                'severity': anomaly_result.get('severity', 'medium'),
                'score': anomaly_result.get('composite_score', 0.0),
                'description': self.generate_alert_description(processed_tweet, anomaly_result),
                'source_tweet': {
                    'id': processed_tweet['original_tweet']['tweet_id'],
                    'text': processed_tweet['original_tweet']['text'][:200],
                    'sentiment': processed_tweet.get('sentiment_label'),
                    'sentiment_score': processed_tweet.get('sentiment_score')
                },
                'anomaly_details': {
                    'volume_score': anomaly_result.get('components', {}).get('volume', {}).get('score', 0),
                    'sentiment_score': anomaly_result.get('components', {}).get('sentiment', {}).get('score', 0),
                    'keyword_score': anomaly_result.get('components', {}).get('keyword', {}).get('score', 0),
                    'engagement_score': anomaly_result.get('components', {}).get('engagement', {}).get('score', 0)
                },
                'recommended_actions': self.get_recommended_actions(anomaly_result),
                'requires_immediate_attention': anomaly_result.get('severity') == 'critical'
            }
            
            return alert
        except Exception as e:
            self.logger.error(f"Error creating alert: {e}")
            return {}

    def generate_alert_description(self, processed_tweet: Dict, anomaly_result: Dict) -> str:
        """Génère une description textuelle de l'alerte"""
        try:
            severity = anomaly_result.get('severity', 'medium')
            score = anomaly_result.get('composite_score', 0.0)
            
            # Identifier les composants principaux de l'anomalie
            components = anomaly_result.get('components', {})
            main_factors = []
            
            for comp_type, comp_data in components.items():
                comp_score = comp_data.get('score', 0)
                if comp_score > 0.5:
                    main_factors.append(comp_type)
            
            factors_text = ", ".join(main_factors) if main_factors else "multiple factors"
            
            description = (
                f"{severity.upper()} anomaly detected (score: {score:.2f}) "
                f"driven by {factors_text}. "
                f"Tweet sentiment: {processed_tweet.get('sentiment_label', 'unknown')} "
                f"({processed_tweet.get('sentiment_score', 0):.2f})"
            )
            
            return description
        except Exception as e:
            self.logger.error(f"Error generating alert description: {e}")
            return f"Anomaly detected with severity: {anomaly_result.get('severity', 'unknown')}"

    def get_recommended_actions(self, anomaly_result: Dict) -> List[str]:
        """Génère des actions recommandées basées sur l'anomalie"""
        try:
            severity = anomaly_result.get('severity', 'medium')
            
            actions = []
            
            if severity == 'critical':
                actions.extend([
                    "Immediate investigation required",
                    "Notify senior analysts",
                    "Monitor news sources for breaking developments",
                    "Assess market impact"
                ])
            elif severity == 'high':
                actions.extend([
                    "Increased monitoring recommended",
                    "Review related tweets and sources",
                    "Prepare situation report"
                ])
            else:
                actions.extend([
                    "Continue monitoring",
                    "Log for trend analysis"
                ])
            
            return actions
        except Exception as e:
            self.logger.error(f"Error getting recommended actions: {e}")
            return ["Monitor situation"]

    def save_processed_tweets(self, processed_tweets: List[Dict]):
        """Sauvegarde les tweets traités dans Elasticsearch"""
        if not self.elasticsearch_client:
            return
            
        try:
            actions = []
            index_name = f"{self.config['elasticsearch_index_tweets']}-{datetime.now().strftime('%Y-%m')}"
            
            for tweet in processed_tweets:
                try:
                    action = {
                        "_index": index_name,
                        "_id": tweet['original_tweet']['tweet_id'],
                        "_source": tweet
                    }
                    actions.append(action)
                except Exception as e:
                    self.logger.error(f"Error preparing tweet for Elasticsearch: {e}")
                    continue
            
            if actions:
                success, failed = bulk(
                    self.elasticsearch_client, 
                    actions, 
                    refresh=True,
                    max_retries=3,
                    initial_backoff=2,
                    max_backoff=600
                )
                self.logger.debug(f"Elasticsearch bulk insert: {success} success, {len(failed)} failed")
                
        except Exception as e:
            self.logger.error(f"Error saving to Elasticsearch: {e}")

    def save_anomalies(self, anomalies: List[Dict]):
        """Sauvegarde les anomalies dans Elasticsearch"""
        if not self.elasticsearch_client:
            return
            
        try:
            actions = []
            index_name = f"{self.config['elasticsearch_index_anomalies']}-{datetime.now().strftime('%Y-%m')}"
            
            for anomaly in anomalies:
                try:
                    doc_id = f"{anomaly.get('timestamp', datetime.now().isoformat())}_{anomaly.get('tweet_id', 'unknown')}"
                    action = {
                        "_index": index_name,
                        "_id": doc_id,
                        "_source": anomaly
                    }
                    actions.append(action)
                except Exception as e:
                    self.logger.error(f"Error preparing anomaly for Elasticsearch: {e}")
                    continue
            
            if actions:
                success, failed = bulk(
                    self.elasticsearch_client, 
                    actions, 
                    refresh=True,
                    max_retries=3,
                    initial_backoff=2,
                    max_backoff=600
                )
                self.logger.debug(f"Elasticsearch anomalies bulk insert: {success} success, {len(failed)} failed")
                
        except Exception as e:
            self.logger.error(f"Error saving anomalies to Elasticsearch: {e}")

    def send_to_kafka(self, data: List[Dict], topic: str):
        """Envoie les données vers un topic Kafka"""
        try:
            sent_count = 0
            for item in data:
                try:
                    # Utiliser l'ID comme clé pour le partitioning
                    key = (item.get('original_tweet', {}).get('tweet_id') or 
                          item.get('tweet_id') or 
                          item.get('alert_id') or 
                          str(datetime.now().timestamp()))
                    
                    self.kafka_producer.send(
                        topic,
                        value=item,
                        key=key
                    )
                    sent_count += 1
                except Exception as e:
                    self.logger.error(f"Error sending individual message to Kafka: {e}")
                    continue
            
            if sent_count > 0:
                self.kafka_producer.flush(timeout=10)
                self.logger.debug(f"Sent {sent_count} messages to topic {topic}")
            
        except Exception as e:
            self.logger.error(f"Error sending to Kafka topic {topic}: {e}")

    def start_processing(self):
        """Démarre le traitement en streaming"""
        try:
            self.logger.info("Starting sentiment analysis stream processing...")
            
            # Créer le stream d'entrée
            tweets_stream = self.create_kafka_stream()
            
            # Configurer le traitement en micro-batches
            query = tweets_stream.writeStream \
                .trigger(processingTime=f'{self.config["batch_duration"]} seconds') \
                .foreachBatch(self.process_tweet_batch) \
                .option("checkpointLocation", self.config["checkpoint_location"]) \
                .outputMode("append") \
                .start()
            
            self.logger.info("Stream processing started successfully")
            
            # Monitoring périodique
            self.start_monitoring_thread()
            
            # Attendre la fin du traitement
            query.awaitTermination()
            
        except Exception as e:
            self.logger.error(f"Error in stream processing: {e}")
            raise
        finally:
            self.cleanup()

    def start_monitoring_thread(self):
        """Démarre le monitoring en arrière-plan"""
        import threading
        import time
        
        def monitor():
            while True:
                try:
                    time.sleep(60)  # Monitoring toutes les minutes
                    
                    self.logger.info(
                        f"Processing stats - Tweets: {self.processed_tweets}, "
                        f"Anomalies: {self.detected_anomalies}, "
                        f"Alerts: {self.alerts_sent}"
                    )
                    
                    # Statistiques des analyseurs
                    if self.anomaly_detector:
                        try:
                            summary = self.anomaly_detector.get_anomaly_summary()
                            if summary.get('total', 0) > 0:
                                self.logger.info(f"Anomaly summary: {summary}")
                        except Exception as e:
                            self.logger.error(f"Error getting anomaly summary: {e}")
                    
                except Exception as e:
                    self.logger.error(f"Error in monitoring: {e}")
        
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()

    def cleanup(self):
        """Nettoyage des ressources"""
        try:
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close(timeout=10)
            
            if hasattr(self, 'spark'):
                self.spark.stop()
                
            self.logger.info("Cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

def main():
    """Point d'entrée principal"""
    processor = None
    try:
        processor = SentimentStreamProcessor()
        processor.start_processing()
        
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logging.error(f"Application error: {e}")
        sys.exit(1)
    finally:
        if processor:
            processor.cleanup()

if __name__ == "__main__":
    main()