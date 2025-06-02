import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from collections import defaultdict, deque
import logging
import json
import math

class TradeAnomalyDetector:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.setup_parameters()
        self.initialize_state()
        
    def setup_parameters(self):
        """Configure les paramètres de détection"""
        self.parameters = {
            # Fenêtres temporelles
            'short_window': 300,      # 5 minutes
            'medium_window': 1800,    # 30 minutes 
            'long_window': 7200,      # 2 heures
            
            # Seuils de détection
            'volume_spike_threshold': 3.0,        # 3x volume normal
            'sentiment_shift_threshold': 0.4,     # Changement de 0.4 en sentiment
            'keyword_anomaly_threshold': 2.5,     # Z-score pour mots-clés
            'engagement_spike_threshold': 4.0,    # 4x engagement normal
            
            # Paramètres statistiques
            'min_samples': 10,         # Minimum d'échantillons pour calcul
            'outlier_percentile': 95,  # Percentile pour détecter outliers
            'smoothing_factor': 0.3,   # Pour moyenne mobile exponentielle
            
            # Scores d'anomalie
            'critical_threshold': 0.8,  # Score critique
            'high_threshold': 0.6,      # Score élevé
            'medium_threshold': 0.4     # Score moyen
        }

    def initialize_state(self):
        """Initialise l'état du détecteur"""
        # Historique des métriques par fenêtre temporelle
        self.metrics_history = {
            'short': {
                'volume': deque(maxlen=100),
                'sentiment': deque(maxlen=100),
                'engagement': deque(maxlen=100),
                'timestamps': deque(maxlen=100)
            },
            'medium': {
                'volume': deque(maxlen=200),
                'sentiment': deque(maxlen=200),
                'engagement': deque(maxlen=200),
                'timestamps': deque(maxlen=200)
            },
            'long': {
                'volume': deque(maxlen=500),
                'sentiment': deque(maxlen=500),
                'engagement': deque(maxlen=500),
                'timestamps': deque(maxlen=500)
            }
        }
        
        # Compteurs pour les mots-clés
        self.keyword_counters = {
            'short': defaultdict(int),
            'medium': defaultdict(int),
            'long': defaultdict(int)
        }
        
        # Historique des mots-clés par fenêtre
        self.keyword_history = {
            'short': deque(maxlen=1000),
            'medium': deque(maxlen=2000),
            'long': deque(maxlen=5000)
        }
        
        # Baseline des métriques (valeurs normales)
        self.baselines = {
            'volume': {'mean': 10.0, 'std': 3.0},
            'sentiment': {'mean': 0.0, 'std': 0.3},
            'engagement': {'mean': 15.0, 'std': 8.0}
        }
        
        # Cache des anomalies détectées récemment
        self.recent_anomalies = deque(maxlen=100)

    def update_metrics(self, tweet_data: Dict) -> None:
        """Met à jour les métriques avec un nouveau tweet"""
        try:
            timestamp_str = tweet_data.get('timestamp', datetime.now().isoformat())
            
            # Gérer différents formats de timestamp
            try:
                if timestamp_str.endswith('Z'):
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    timestamp = datetime.fromisoformat(timestamp_str)
            except Exception:
                timestamp = datetime.now()
            
            # Extraction des métriques du tweet
            metrics = self.extract_tweet_metrics(tweet_data)
            
            # Mise à jour pour chaque fenêtre temporelle
            for window in ['short', 'medium', 'long']:
                # Nettoyer les anciennes données
                self.cleanup_old_data(window, timestamp)
                
                # Ajouter les nouvelles métriques
                self.metrics_history[window]['volume'].append(metrics['volume'])
                self.metrics_history[window]['sentiment'].append(metrics['sentiment'])
                self.metrics_history[window]['engagement'].append(metrics['engagement'])
                self.metrics_history[window]['timestamps'].append(timestamp)
                
                # Mettre à jour les compteurs de mots-clés
                for keyword in metrics['keywords']:
                    self.keyword_counters[window][keyword] += 1
                
                # Ajouter à l'historique des mots-clés
                if metrics['keywords']:
                    self.keyword_history[window].extend(metrics['keywords'])
            
            # Mettre à jour les baselines périodiquement
            if len(self.metrics_history['long']['volume']) % 100 == 0:
                self.update_baselines()
                
        except Exception as e:
            self.logger.error(f"Error updating metrics: {e}")

    def extract_tweet_metrics(self, tweet_data: Dict) -> Dict:
        """Extrait les métriques pertinentes d'un tweet"""
        try:
            # Volume (pondéré par priorité et engagement)
            base_volume = 1.0
            priority_multiplier = {
                'low': 0.5,
                'normal': 1.0,
                'high': 2.0,
                'critical': 3.0
            }.get(tweet_data.get('priority', 'normal'), 1.0)
            
            engagement = tweet_data.get('engagement', {})
            if not engagement:
                engagement = {}
                
            engagement_score = (
                engagement.get('likes', 0) * 0.3 +
                engagement.get('retweets', 0) * 0.5 +
                engagement.get('replies', 0) * 0.2
            )
            
            volume = base_volume * priority_multiplier * (1 + engagement_score / 100)
            
            # Sentiment
            sentiment_data = tweet_data.get('sentiment', {})
            if not sentiment_data:
                sentiment_data = {}
            sentiment_score = sentiment_data.get('score', 0.0)
            
            # Mots-clés (nettoyés et filtrés)
            keywords = tweet_data.get('keywords', [])
            entities = tweet_data.get('entities', {})
            
            if not keywords:
                keywords = []
            if not entities:
                entities = {}
            
            # Ajouter les entreprises et pays comme mots-clés
            all_keywords = keywords.copy()
            all_keywords.extend(entities.get('companies', []))
            all_keywords.extend(entities.get('countries', []))
            
            # Filtrer et nettoyer
            clean_keywords = []
            for kw in all_keywords:
                if kw and isinstance(kw, str) and len(kw) > 2:
                    clean_kw = kw.lower().strip()
                    if clean_kw.replace('-', '').replace('_', '').isalnum():
                        clean_keywords.append(clean_kw)
            
            return {
                'volume': volume,
                'sentiment': sentiment_score,
                'engagement': engagement_score,
                'keywords': clean_keywords,
                'is_crisis': tweet_data.get('is_anomaly', False),
                'urgency_score': tweet_data.get('features', {}).get('urgency_score', 0.0) if tweet_data.get('features') else 0.0
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting tweet metrics: {e}")
            return {
                'volume': 1.0,
                'sentiment': 0.0,
                'engagement': 0.0,
                'keywords': [],
                'is_crisis': False,
                'urgency_score': 0.0
            }

    def cleanup_old_data(self, window: str, current_time: datetime) -> None:
        """Nettoie les données trop anciennes pour la fenêtre donnée"""
        try:
            window_size = self.parameters[f'{window}_window']
            cutoff_time = current_time - timedelta(seconds=window_size)
            
            timestamps = self.metrics_history[window]['timestamps']
            
            # Vérifier que la deque n'est pas vide
            if not timestamps:
                return
            
            # Compter combien d'éléments à supprimer
            remove_count = 0
            for ts in timestamps:
                if ts < cutoff_time:
                    remove_count += 1
                else:
                    break
            
            # Supprimer les anciens éléments de manière sécurisée
            if remove_count > 0:
                max_removable = min(remove_count, len(timestamps))
                for _ in range(max_removable):
                    if timestamps:  # Double vérification
                        timestamps.popleft()
                    if self.metrics_history[window]['volume']:
                        self.metrics_history[window]['volume'].popleft()
                    if self.metrics_history[window]['sentiment']:
                        self.metrics_history[window]['sentiment'].popleft()
                    if self.metrics_history[window]['engagement']:
                        self.metrics_history[window]['engagement'].popleft()
            
            # Nettoyer les compteurs de mots-clés (décroissance exponentielle)
            decay_factor = 0.95
            keywords_to_remove = []
            for keyword in self.keyword_counters[window]:
                self.keyword_counters[window][keyword] *= decay_factor
                if self.keyword_counters[window][keyword] < 0.1:
                    keywords_to_remove.append(keyword)
            
            for keyword in keywords_to_remove:
                del self.keyword_counters[window][keyword]
                    
        except Exception as e:
            self.logger.error(f"Error cleaning old data for {window}: {e}")

    def update_baselines(self) -> None:
        """Met à jour les baselines basées sur l'historique long"""
        try:
            long_history = self.metrics_history['long']
            
            for metric in ['volume', 'sentiment', 'engagement']:
                data = list(long_history[metric])
                if len(data) >= self.parameters['min_samples']:
                    # Exclure les outliers pour un baseline plus stable
                    q1, q3 = np.percentile(data, [25, 75])
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    
                    filtered_data = [x for x in data if lower_bound <= x <= upper_bound]
                    
                    if filtered_data:
                        self.baselines[metric]['mean'] = np.mean(filtered_data)
                        self.baselines[metric]['std'] = max(np.std(filtered_data), 0.01)  # Protection division par zéro
                        
        except Exception as e:
            self.logger.error(f"Error updating baselines: {e}")

    def detect_volume_anomaly(self, window: str = 'short') -> Dict:
        """Détecte les anomalies de volume"""
        try:
            volume_data = list(self.metrics_history[window]['volume'])
            
            if len(volume_data) < self.parameters['min_samples']:
                return {'type': 'volume', 'score': 0.0, 'details': 'insufficient_data'}
            
            # Volume actuel (moyenne des 3 derniers points)
            current_volume = np.mean(volume_data[-3:])
            
            # Volume baseline
            baseline_mean = self.baselines['volume']['mean']
            baseline_std = max(self.baselines['volume']['std'], 0.01)  # Protection division par zéro
            
            # Z-score
            z_score = (current_volume - baseline_mean) / baseline_std
            
            # Score d'anomalie (0-1)
            volume_score = min(abs(z_score) / 4.0, 1.0)
            
            # Détection de spike
            is_spike = z_score > self.parameters['volume_spike_threshold']
            
            return {
                'type': 'volume',
                'score': volume_score,
                'is_spike': is_spike,
                'details': {
                    'current_volume': current_volume,
                    'baseline_mean': baseline_mean,
                    'z_score': z_score,
                    'threshold': self.parameters['volume_spike_threshold']
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting volume anomaly: {e}")
            return {'type': 'volume', 'score': 0.0, 'error': str(e)}

    def detect_sentiment_anomaly(self, window: str = 'short') -> Dict:
        """Détecte les anomalies de sentiment"""
        try:
            sentiment_data = list(self.metrics_history[window]['sentiment'])
            
            if len(sentiment_data) < self.parameters['min_samples']:
                return {'type': 'sentiment', 'score': 0.0, 'details': 'insufficient_data'}
            
            # Sentiment actuel et historique
            current_sentiment = np.mean(sentiment_data[-5:])  # Moyenne des 5 derniers
            historical_sentiment = np.mean(sentiment_data[:-5]) if len(sentiment_data) > 5 else 0.0
            
            # Changement de sentiment
            sentiment_shift = abs(current_sentiment - historical_sentiment)
            
            # Score basé sur l'amplitude du changement
            shift_score = min(sentiment_shift / 0.8, 1.0)  # Normalisation
            
            # Détection de changement soudain
            is_sudden_shift = sentiment_shift > self.parameters['sentiment_shift_threshold']
            
            # Score d'anomalie renforcé si le sentiment est extrême
            extreme_sentiment_bonus = 0.0
            if abs(current_sentiment) > 0.7:
                extreme_sentiment_bonus = 0.2
            
            final_score = min(shift_score + extreme_sentiment_bonus, 1.0)
            
            return {
                'type': 'sentiment',
                'score': final_score,
                'is_sudden_shift': is_sudden_shift,
                'details': {
                    'current_sentiment': current_sentiment,
                    'historical_sentiment': historical_sentiment,
                    'sentiment_shift': sentiment_shift,
                    'threshold': self.parameters['sentiment_shift_threshold']
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting sentiment anomaly: {e}")
            return {'type': 'sentiment', 'score': 0.0, 'error': str(e)}

    def detect_keyword_anomaly(self, window: str = 'short') -> Dict:
        """Détecte les anomalies dans les mots-clés"""
        try:
            keyword_counts = dict(self.keyword_counters[window])
            
            if not keyword_counts:
                return {'type': 'keyword', 'score': 0.0, 'details': 'no_keywords'}
            
            # Calculer les statistiques des mots-clés
            counts = list(keyword_counts.values())
            mean_count = np.mean(counts)
            std_count = max(np.std(counts), 0.1) if len(counts) > 1 else 1.0  # Protection division par zéro
            
            # Identifier les mots-clés anormalement fréquents
            anomalous_keywords = []
            max_z_score = 0.0
            
            for keyword, count in keyword_counts.items():
                if std_count > 0:
                    z_score = (count - mean_count) / std_count
                    if z_score > self.parameters['keyword_anomaly_threshold']:
                        anomalous_keywords.append({
                            'keyword': keyword,
                            'count': count,
                            'z_score': z_score
                        })
                        max_z_score = max(max_z_score, z_score)
            
            # Score d'anomalie basé sur le z-score maximum
            keyword_score = min(max_z_score / 5.0, 1.0) if max_z_score > 0 else 0.0
            
            # Boost si mots-clés de crise
            crisis_keywords = ['crisis', 'emergency', 'collapse', 'breakdown', 'urgent', 'critical']
            crisis_boost = 0.0
            for kw_data in anomalous_keywords:
                if any(crisis_kw in kw_data['keyword'].lower() for crisis_kw in crisis_keywords):
                    crisis_boost = 0.3
                    break
            
            final_score = min(keyword_score + crisis_boost, 1.0)
            
            return {
                'type': 'keyword',
                'score': final_score,
                'anomalous_keywords': anomalous_keywords[:5],  # Top 5
                'details': {
                    'total_keywords': len(keyword_counts),
                    'mean_count': mean_count,
                    'max_z_score': max_z_score,
                    'crisis_boost': crisis_boost
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting keyword anomaly: {e}")
            return {'type': 'keyword', 'score': 0.0, 'error': str(e)}

    def detect_engagement_anomaly(self, window: str = 'short') -> Dict:
        """Détecte les anomalies d'engagement"""
        try:
            engagement_data = list(self.metrics_history[window]['engagement'])
            
            if len(engagement_data) < self.parameters['min_samples']:
                return {'type': 'engagement', 'score': 0.0, 'details': 'insufficient_data'}
            
            # Engagement actuel
            current_engagement = np.mean(engagement_data[-3:])
            
            # Baseline d'engagement
            baseline_mean = self.baselines['engagement']['mean']
            baseline_std = max(self.baselines['engagement']['std'], 0.01)  # Protection division par zéro
            
            # Z-score
            z_score = (current_engagement - baseline_mean) / baseline_std
            
            # Score d'anomalie
            engagement_score = min(abs(z_score) / 4.0, 1.0)
            
            # Détection de spike
            is_spike = z_score > self.parameters['engagement_spike_threshold']
            
            return {
                'type': 'engagement',
                'score': engagement_score,
                'is_spike': is_spike,
                'details': {
                    'current_engagement': current_engagement,
                    'baseline_mean': baseline_mean,
                    'z_score': z_score,
                    'threshold': self.parameters['engagement_spike_threshold']
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting engagement anomaly: {e}")
            return {'type': 'engagement', 'score': 0.0, 'error': str(e)}

    def detect_anomalies(self, tweet_data: Dict) -> Dict:
        """Détection principale d'anomalies"""
        try:
            # Mettre à jour les métriques avec le nouveau tweet
            self.update_metrics(tweet_data)
            
            # Détecter les différents types d'anomalies
            volume_anomaly = self.detect_volume_anomaly('short')
            sentiment_anomaly = self.detect_sentiment_anomaly('short')
            keyword_anomaly = self.detect_keyword_anomaly('short')
            engagement_anomaly = self.detect_engagement_anomaly('short')
            
            # Calcul du score composite
            anomaly_scores = [
                volume_anomaly['score'] * 0.3,      # 30% volume
                sentiment_anomaly['score'] * 0.25,   # 25% sentiment
                keyword_anomaly['score'] * 0.25,     # 25% mots-clés
                engagement_anomaly['score'] * 0.2    # 20% engagement
            ]
            
            composite_score = sum(anomaly_scores)
            
            # Classification du niveau d'anomalie
            if composite_score >= self.parameters['critical_threshold']:
                severity = 'critical'
                alert_required = True
            elif composite_score >= self.parameters['high_threshold']:
                severity = 'high'
                alert_required = True
            elif composite_score >= self.parameters['medium_threshold']:
                severity = 'medium'
                alert_required = False
            else:
                severity = 'low'
                alert_required = False
            
            # Boost si le tweet est déjà marqué comme crise
            if tweet_data.get('is_anomaly', False):
                composite_score = min(composite_score * 1.5, 1.0)
                if severity != 'critical':
                    severity = 'critical'
                alert_required = True
            
            # Créer le résultat d'anomalie
            anomaly_result = {
                'timestamp': datetime.now().isoformat(),
                'tweet_id': tweet_data.get('tweet_id'),
                'composite_score': composite_score,
                'severity': severity,
                'alert_required': alert_required,
                'components': {
                    'volume': volume_anomaly,
                    'sentiment': sentiment_anomaly,
                    'keyword': keyword_anomaly,
                    'engagement': engagement_anomaly
                },
                'context': {
                    'original_tweet': tweet_data.get('text', '')[:200],
                    'tweet_sentiment': tweet_data.get('sentiment', {}),
                    'tweet_keywords': tweet_data.get('keywords', []),
                    'current_baselines': self.baselines.copy()
                }
            }
            
            # Ajouter à l'historique des anomalies si significative
            if composite_score >= self.parameters['medium_threshold']:
                self.recent_anomalies.append(anomaly_result)
            
            return anomaly_result
            
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'composite_score': 0.0,
                'severity': 'low',
                'alert_required': False,
                'error': str(e)
            }

    def get_anomaly_summary(self, window: str = 'short') -> Dict:
        """Retourne un résumé des anomalies récentes"""
        try:
            recent_time = datetime.now() - timedelta(minutes=30)
            recent_anomalies = []
            
            for anomaly in self.recent_anomalies:
                try:
                    anomaly_time = datetime.fromisoformat(anomaly['timestamp'])
                    if anomaly_time > recent_time:
                        recent_anomalies.append(anomaly)
                except Exception:
                    continue
            
            if not recent_anomalies:
                return {'total': 0, 'by_severity': {}, 'trend': 'stable'}
            
            # Compter par sévérité
            severity_counts = defaultdict(int)
            for anomaly in recent_anomalies:
                severity_counts[anomaly['severity']] += 1
            
            # Tendance (basée sur les 10 dernières vs les 10 précédentes)
            if len(recent_anomalies) >= 20:
                recent_scores = [a['composite_score'] for a in recent_anomalies[-10:]]
                previous_scores = [a['composite_score'] for a in recent_anomalies[-20:-10]]
                
                recent_avg = np.mean(recent_scores)
                previous_avg = np.mean(previous_scores)
                
                if recent_avg > previous_avg * 1.2:
                    trend = 'increasing'
                elif recent_avg < previous_avg * 0.8:
                    trend = 'decreasing'
                else:
                    trend = 'stable'
            else:
                trend = 'insufficient_data'
            
            return {
                'total': len(recent_anomalies),
                'by_severity': dict(severity_counts),
                'trend': trend,
                'average_score': np.mean([a['composite_score'] for a in recent_anomalies]),
                'max_score': max([a['composite_score'] for a in recent_anomalies])
            }
            
        except Exception as e:
            self.logger.error(f"Error generating anomaly summary: {e}")
            return {'error': str(e)}

    def reset_state(self):
        """Remet à zéro l'état du détecteur"""
        self.initialize_state()
        self.logger.info("Anomaly detector state reset")