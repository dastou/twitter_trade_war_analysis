import os
import json
import logging
import time
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict, deque
import threading
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Imports Kafka et Elasticsearch
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

class AlertManager:
    def __init__(self):
        self.setup_logging()
        self.load_config()
        self.setup_external_connections()
        self.initialize_state()
        
        # Gestion des signaux pour arrêt propre
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.running = False

    def setup_logging(self):
        """Configure le logging"""
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)

    def load_config(self):
        """Charge la configuration"""
        self.config = {
            # Kafka
            'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'kafka_alert_topic': os.getenv('KAFKA_TOPIC_ALERTS', 'alerts'),
            'kafka_anomaly_topic': os.getenv('KAFKA_TOPIC_ANOMALIES', 'anomalies'),
            'kafka_consumer_group': 'alert-manager-group',
            
            # Elasticsearch
            'elasticsearch_host': os.getenv('ELASTICSEARCH_HOST', 'elasticsearch:9200'),
            'elasticsearch_index_alerts': 'alerts',
            
            # Grafana
            'grafana_url': os.getenv('GRAFANA_URL', 'http://grafana:3000'),
            'grafana_api_key': os.getenv('GRAFANA_API_KEY', ''),
            'grafana_dashboard_uid': 'sentiment-dashboard',
            
            # Alertes
            'alert_cooldown_minutes': int(os.getenv('ALERT_COOLDOWN_MINUTES', '15')),
            'max_alerts_per_hour': int(os.getenv('MAX_ALERTS_PER_HOUR', '10')),
            'enable_email_alerts': os.getenv('ENABLE_EMAIL_ALERTS', 'false').lower() == 'true',
            'enable_grafana_notifications': os.getenv('ENABLE_GRAFANA_NOTIFICATIONS', 'true').lower() == 'true',
            
            # Email (si activé)
            'smtp_server': os.getenv('SMTP_SERVER', ''),
            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
            'smtp_username': os.getenv('SMTP_USERNAME', ''),
            'smtp_password': os.getenv('SMTP_PASSWORD', ''),
            'alert_recipients': [email.strip() for email in os.getenv('ALERT_RECIPIENTS', '').split(',') if email.strip()]
        }

    def setup_external_connections(self):
        """Configure les connexions externes"""
        try:
            # Consumer Kafka pour les alertes
            self.kafka_consumer = KafkaConsumer(
                self.config['kafka_alert_topic'],
                self.config['kafka_anomaly_topic'],
                bootstrap_servers=self.config['kafka_bootstrap_servers'].split(','),
                group_id=self.config['kafka_consumer_group'],
                value_deserializer=lambda m: self.safe_json_deserialize(m),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                #consumer_timeout_ms=5000  # Timeout pour éviter les blocages
            )
            
            # Client Elasticsearch
            try:
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
            except Exception as e:
                self.logger.warning(f"Elasticsearch connection failed: {e}, continuing without it")
                self.elasticsearch_client = None
            
            self.logger.info("External connections established")
            
        except Exception as e:
            self.logger.error(f"Failed to setup connections: {e}")
            raise

    def safe_json_deserialize(self, message):
        """Désérialise JSON de manière sécurisée"""
        try:
            if message is None:
                return {}
            return json.loads(message.decode('utf-8'))
        except Exception as e:
            self.logger.error(f"Error deserializing message: {e}")
            return {}

    def initialize_state(self):
        """Initialise l'état du gestionnaire d'alertes"""
        # Historique des alertes pour éviter les doublons
        self.alert_history = deque(maxlen=1000)
        
        # Compteurs pour la limitation de débit
        self.hourly_alert_count = defaultdict(int)
        self.last_alert_cleanup = datetime.now()
        
        # Cache des alertes par type pour cooldown
        self.alert_cooldown_cache = {}
        
        # Statistiques
        self.stats = {
            'total_alerts_processed': 0,
            'total_alerts_sent': 0,
            'total_alerts_filtered': 0,
            'start_time': datetime.now()
        }

    def should_send_alert(self, alert: Dict) -> Dict[str, any]:
        """Détermine si une alerte doit être envoyée"""
        try:
            alert_type = alert.get('type', 'unknown')
            severity = alert.get('severity', 'medium')
            current_time = datetime.now()
            
            decision = {
                'should_send': True,
                'reasons': [],
                'filters_applied': []
            }
            
            # 1. Vérifier le cooldown par type d'alerte
            cooldown_key = f"{alert_type}_{severity}"
            if cooldown_key in self.alert_cooldown_cache:
                last_sent = self.alert_cooldown_cache[cooldown_key]
                cooldown_period = timedelta(minutes=self.config['alert_cooldown_minutes'])
                
                if current_time - last_sent < cooldown_period:
                    decision['should_send'] = False
                    decision['reasons'].append(f"Cooldown active for {cooldown_key}")
                    decision['filters_applied'].append('cooldown')
            
            # 2. Vérifier la limite horaire
            current_hour = current_time.hour
            if self.hourly_alert_count[current_hour] >= self.config['max_alerts_per_hour']:
                # Exception pour les alertes critiques
                if severity != 'critical':
                    decision['should_send'] = False
                    decision['reasons'].append(f"Hourly limit reached ({self.config['max_alerts_per_hour']})")
                    decision['filters_applied'].append('rate_limit')
            
            # 3. Vérifier les doublons récents (contenu similaire)
            alert_content = str(alert.get('description', '')) + str(alert.get('score', 0))
            for recent_alert in list(self.alert_history)[-10:]:  # Dernières 10 alertes
                if self.calculate_similarity(alert_content, recent_alert.get('content', '')) > 0.8:
                    decision['should_send'] = False
                    decision['reasons'].append("Similar alert recently sent")
                    decision['filters_applied'].append('duplicate')
                    break
            
            # 4. Les alertes critiques passent la plupart des filtres
            if severity == 'critical':
                if 'cooldown' in decision['filters_applied']:
                    # Réduire le cooldown pour les alertes critiques
                    if current_time - self.alert_cooldown_cache.get(cooldown_key, datetime.min) > timedelta(minutes=5):
                        decision['should_send'] = True
                        decision['reasons'] = ["Critical alert overrides cooldown"]
                        decision['filters_applied'] = []
            
            return decision
        except Exception as e:
            self.logger.error(f"Error in should_send_alert: {e}")
            return {'should_send': True, 'reasons': ['Error in filtering'], 'filters_applied': []}

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calcule la similarité entre deux textes"""
        try:
            if not text1 or not text2:
                return 0.0
            
            # Similarité simple basée sur les mots communs
            words1 = set(str(text1).lower().split())
            words2 = set(str(text2).lower().split())
            
            if not words1 or not words2:
                return 0.0
            
            intersection = words1 & words2
            union = words1 | words2
            
            return len(intersection) / len(union) if union else 0.0
        except Exception as e:
            self.logger.error(f"Error calculating similarity: {e}")
            return 0.0

    def process_alert(self, alert: Dict) -> bool:
        """Traite une alerte individuelle"""
        try:
            self.stats['total_alerts_processed'] += 1
            alert_id = alert.get('alert_id', f"alert_{int(time.time())}")
            
            self.logger.info(f"Processing alert {alert_id}: {alert.get('severity', 'unknown')} - {str(alert.get('description', ''))[:100]}")
            
            # Enrichir l'alerte
            enriched_alert = self.enrich_alert(alert)
            
            # Vérifier si l'alerte doit être envoyée
            send_decision = self.should_send_alert(enriched_alert)
            
            if not send_decision['should_send']:
                self.logger.info(f"Alert {alert_id} filtered: {', '.join(send_decision['reasons'])}")
                self.stats['total_alerts_filtered'] += 1
                return False
            
            # Sauvegarder l'alerte
            self.save_alert_to_elasticsearch(enriched_alert)
            
            # Envoyer les notifications
            notifications_sent = []
            
            if self.config['enable_grafana_notifications']:
                if self.send_grafana_notification(enriched_alert):
                    notifications_sent.append('grafana')
            
            if self.config['enable_email_alerts']:
                if self.send_email_notification(enriched_alert):
                    notifications_sent.append('email')
            
            # Mettre à jour l'état
            self.update_alert_state(enriched_alert, notifications_sent)
            
            self.stats['total_alerts_sent'] += 1
            self.logger.info(f"Alert {alert_id} sent successfully via: {', '.join(notifications_sent) if notifications_sent else 'none'}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing alert: {e}")
            return False

    def enrich_alert(self, alert: Dict) -> Dict:
        """Enrichit l'alerte avec des informations contextuelles"""
        try:
            enriched = alert.copy()
            
            # Ajouter timestamp de traitement
            enriched['processing_timestamp'] = datetime.now().isoformat()
            
            # Ajouter contexte temporel
            enriched['time_context'] = {
                'hour': datetime.now().hour,
                'weekday': datetime.now().weekday(),
                'is_business_hours': 9 <= datetime.now().hour <= 17 and datetime.now().weekday() < 5
            }
            
            # Calculer la priorité finale
            enriched['final_priority'] = self.calculate_final_priority(alert)
            
            # Ajouter tags pour classification
            enriched['tags'] = self.generate_alert_tags(alert)
            
            # Ajouter URL du dashboard pour référence
            enriched['dashboard_url'] = f"{self.config['grafana_url']}/d/{self.config['grafana_dashboard_uid']}"
            
            return enriched
        except Exception as e:
            self.logger.error(f"Error enriching alert: {e}")
            return alert

    def calculate_final_priority(self, alert: Dict) -> str:
        """Calcule la priorité finale basée sur plusieurs facteurs"""
        try:
            base_severity = alert.get('severity', 'medium')
            score = alert.get('score', 0.0)
            
            # Facteurs d'ajustement
            priority_score = {
                'low': 1,
                'medium': 2,
                'high': 3,
                'critical': 4
            }.get(base_severity, 2)
            
            # Boost si score très élevé
            if score > 0.9:
                priority_score += 1
            
            # Boost si en heures d'ouverture des marchés
            current_hour = datetime.now().hour
            if 9 <= current_hour <= 17 and datetime.now().weekday() < 5:
                priority_score += 0.5
            
            # Reconvertir en priorité
            if priority_score >= 4:
                return 'critical'
            elif priority_score >= 3:
                return 'high'
            elif priority_score >= 2:
                return 'medium'
            else:
                return 'low'
        except Exception as e:
            self.logger.error(f"Error calculating final priority: {e}")
            return alert.get('severity', 'medium')

    def generate_alert_tags(self, alert: Dict) -> List[str]:
        """Génère des tags pour l'alerte"""
        try:
            tags = []
            
            # Tags basés sur le type
            alert_type = str(alert.get('type', ''))
            if 'sentiment' in alert_type:
                tags.append('sentiment')
            if 'trade' in alert_type:
                tags.append('trade')
            if 'anomaly' in alert_type:
                tags.append('anomaly')
            
            # Tags basés sur la sévérité
            tags.append(f"severity:{alert.get('severity', 'unknown')}")
            
            # Tags basés sur le contenu
            description = str(alert.get('description', '')).lower()
            if 'volume' in description:
                tags.append('volume-spike')
            if 'sentiment' in description:
                tags.append('sentiment-shift')
            
            # Tags temporels
            current_time = datetime.now()
            if 9 <= current_time.hour <= 17:
                tags.append('business-hours')
            else:
                tags.append('after-hours')
            
            return tags
        except Exception as e:
            self.logger.error(f"Error generating alert tags: {e}")
            return ['unknown']

    def save_alert_to_elasticsearch(self, alert: Dict):
        """Sauvegarde l'alerte dans Elasticsearch"""
        if not self.elasticsearch_client:
            return
            
        try:
            index_name = f"{self.config['elasticsearch_index_alerts']}-{datetime.now().strftime('%Y-%m')}"
            
            doc_id = alert.get('alert_id', f"alert_{int(time.time())}")
            
            self.elasticsearch_client.index(
                index=index_name,
                id=doc_id,
                document=alert,
                refresh=True
            )
            
            self.logger.debug(f"Alert saved to Elasticsearch: {doc_id}")
            
        except Exception as e:
            self.logger.error(f"Error saving alert to Elasticsearch: {e}")

    def send_grafana_notification(self, alert: Dict) -> bool:
        """Envoie une notification à Grafana"""
        try:
            if not self.config['grafana_api_key']:
                self.logger.warning("Grafana API key not configured")
                return False
            
            # Créer une annotation Grafana
            annotation = {
                'time': int(datetime.now().timestamp() * 1000),
                'timeEnd': int((datetime.now() + timedelta(minutes=5)).timestamp() * 1000),
                'tags': alert.get('tags', []),
                'text': str(alert.get('description', 'Trade sentiment anomaly detected')),
                'title': f"Alert: {str(alert.get('severity', 'unknown')).upper()}",
                'dashboardUID': self.config['grafana_dashboard_uid']
            }
            
            headers = {
                'Authorization': f"Bearer {self.config['grafana_api_key']}",
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                f"{self.config['grafana_url']}/api/annotations",
                json=annotation,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                self.logger.debug("Grafana annotation created successfully")
                return True
            else:
                self.logger.error(f"Failed to create Grafana annotation: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error sending Grafana notification: {e}")
            return False

    def send_email_notification(self, alert: Dict) -> bool:
        """Envoie une notification par email"""
        try:
            if not self.config['enable_email_alerts'] or not self.config['smtp_server']:
                return False
            
            if not self.config['alert_recipients']:
                self.logger.warning("No email recipients configured")
                return False
            
            # Créer le message
            msg = MIMEMultipart()
            msg['From'] = self.config['smtp_username']
            msg['To'] = ', '.join(self.config['alert_recipients'])
            msg['Subject'] = f"Trade Sentiment Alert: {str(alert.get('severity', '')).upper()}"
            
            # Corps du message
            body = self.generate_email_body(alert)
            msg.attach(MIMEText(body, 'html'))
            
            # Envoyer l'email avec gestion d'erreur robuste
            with smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port']) as server:
                server.starttls()
                server.login(self.config['smtp_username'], self.config['smtp_password'])
                
                text = msg.as_string()
                server.sendmail(self.config['smtp_username'], self.config['alert_recipients'], text)
            
            self.logger.debug("Email notification sent successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending email notification: {e}")
            return False

    def generate_email_body(self, alert: Dict) -> str:
        """Génère le corps HTML de l'email d'alerte"""
        try:
            severity = str(alert.get('severity', 'unknown'))
            score = alert.get('score', 0.0)
            description = str(alert.get('description', 'No description'))
            timestamp = str(alert.get('timestamp', datetime.now().isoformat()))
            dashboard_url = str(alert.get('dashboard_url', ''))
            
            # Couleur selon la sévérité
            color_map = {
                'low': '#28a745',
                'medium': '#ffc107', 
                'high': '#fd7e14',
                'critical': '#dc3545'
            }
            color = color_map.get(severity, '#6c757d')
            
            # Source tweet avec protection
            source_tweet = alert.get('source_tweet', {})
            tweet_text = str(source_tweet.get('text', 'N/A')) if source_tweet else 'N/A'
            
            # Actions recommandées avec protection
            recommended_actions = alert.get('recommended_actions', ['Monitor situation'])
            if not isinstance(recommended_actions, list):
                recommended_actions = ['Monitor situation']
            
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; margin: 20px;">
                <div style="border-left: 4px solid {color}; padding-left: 20px;">
                    <h2 style="color: {color}; margin-top: 0;">
                        Trade Sentiment Alert - {severity.upper()}
                    </h2>
                    
                    <p><strong>Timestamp:</strong> {timestamp}</p>
                    <p><strong>Severity:</strong> {severity}</p>
                    <p><strong>Score:</strong> {score:.3f}</p>
                    
                    <h3>Description:</h3>
                    <p style="background-color: #f8f9fa; padding: 15px; border-radius: 5px;">
                        {description}
                    </p>
                    
                    <h3>Source Tweet:</h3>
                    <div style="background-color: #e9ecef; padding: 10px; border-radius: 5px; font-style: italic;">
                        "{tweet_text}"
                    </div>
                    
                    <h3>Recommended Actions:</h3>
                    <ul>
            """
            
            for action in recommended_actions:
                html_body += f"<li>{str(action)}</li>"
            
            html_body += f"""
                    </ul>
                    
                    {f'<p><a href="{dashboard_url}" style="color: {color};">View Dashboard</a></p>' if dashboard_url else ''}
                    
                    <hr style="margin-top: 30px;">
                    <p style="color: #6c757d; font-size: 12px;">
                        This is an automated alert from the Trade Sentiment Analysis System.
                    </p>
                </div>
            </body>
            </html>
            """
            
            return html_body
        except Exception as e:
            self.logger.error(f"Error generating email body: {e}")
            return f"<html><body><h2>Alert: {alert.get('severity', 'unknown')}</h2><p>Error generating email content</p></body></html>"

    def update_alert_state(self, alert: Dict, notifications_sent: List[str]):
        """Met à jour l'état après envoi d'une alerte"""
        try:
            # Mettre à jour le cache de cooldown
            alert_type = str(alert.get('type', 'unknown'))
            severity = str(alert.get('severity', 'medium'))
            cooldown_key = f"{alert_type}_{severity}"
            self.alert_cooldown_cache[cooldown_key] = datetime.now()
            
            # Mettre à jour le compteur horaire
            current_hour = datetime.now().hour
            self.hourly_alert_count[current_hour] += 1
            
            # Ajouter à l'historique
            alert_summary = {
                'content': str(alert.get('description', '')) + str(alert.get('score', 0)),
                'timestamp': datetime.now(),
                'type': alert_type,
                'severity': severity,
                'notifications': notifications_sent
            }
            self.alert_history.append(alert_summary)
        except Exception as e:
            self.logger.error(f"Error updating alert state: {e}")

    def cleanup_hourly_counters(self):
        """Nettoie les compteurs horaires anciens"""
        try:
            current_time = datetime.now()
            if current_time - self.last_alert_cleanup > timedelta(hours=1):
                # Garder seulement les compteurs des 24 dernières heures
                current_hour = current_time.hour
                hours_to_keep = [(current_hour - i) % 24 for i in range(24)]
                
                old_keys = [h for h in self.hourly_alert_count.keys() if h not in hours_to_keep]
                for key in old_keys:
                    del self.hourly_alert_count[key]
                
                self.last_alert_cleanup = current_time
        except Exception as e:
            self.logger.error(f"Error cleaning hourly counters: {e}")

    def start_processing(self):
        """Démarre le traitement des alertes"""
        self.logger.info("Starting alert processing...")
        self.running = True
        
        # Démarrer le thread de nettoyage
        cleanup_thread = threading.Thread(target=self.periodic_cleanup, daemon=True)
        cleanup_thread.start()
        
        try:
            for message in self.kafka_consumer:
                if not self.running:
                    break
                
                try:
                    alert_data = message.value
                    
                    if not alert_data or not isinstance(alert_data, dict):
                        continue
                    
                    # Traiter l'alerte
                    self.process_alert(alert_data)
                    
                    # Nettoyage périodique
                    self.cleanup_hourly_counters()
                    
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error in main processing loop: {e}")
        finally:
            self.cleanup()

    def periodic_cleanup(self):
        """Nettoyage périodique en arrière-plan"""
        while self.running:
            try:
                time.sleep(300)  # Toutes les 5 minutes
                
                # Log des statistiques
                uptime = datetime.now() - self.stats['start_time']
                self.logger.info(
                    f"Alert Manager Stats - Processed: {self.stats['total_alerts_processed']}, "
                    f"Sent: {self.stats['total_alerts_sent']}, "
                    f"Filtered: {self.stats['total_alerts_filtered']}, "
                    f"Uptime: {uptime}"
                )
                
                # Nettoyage des caches
                self.cleanup_hourly_counters()
                
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}")

    def signal_handler(self, signum, frame):
        """Gestionnaire de signaux pour arrêt propre"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def cleanup(self):
        """Nettoyage des ressources"""
        try:
            if hasattr(self, 'kafka_consumer'):
                self.kafka_consumer.close()
            
            self.logger.info("Alert Manager cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

def main():
    """Point d'entrée principal"""
    alert_manager = None
    try:
        alert_manager = AlertManager()
        alert_manager.start_processing()
        
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt")
    except Exception as e:
        logging.error(f"Application error: {e}")
        sys.exit(1)
    finally:
        if alert_manager:
            alert_manager.cleanup()

if __name__ == "__main__":
    main()