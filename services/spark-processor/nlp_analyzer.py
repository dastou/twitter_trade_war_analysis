import re
import string
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from datetime import datetime
import logging
from collections import Counter
import warnings
warnings.filterwarnings("ignore")

# Imports pour l'analyse de sentiment (VERSION SIMPLIFIÉE)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification  # COMMENTÉ
# import spacy  # COMMENTÉ
# from spacy.lang.en.stop_words import STOP_WORDS as EN_STOP_WORDS  # COMMENTÉ
# from spacy.lang.fr.stop_words import STOP_WORDS as FR_STOP_WORDS  # COMMENTÉ

# Import pour les statistiques textuelles
import textstat

# Stop words de base (sans spaCy)
EN_STOP_WORDS = {'the', 'and', 'is', 'are', 'of', 'to', 'in', 'on', 'with', 'for', 'at', 'by', 'a', 'an', 'as', 'be', 'or', 'but', 'not', 'this', 'that', 'from', 'they', 'we', 'have', 'had', 'will', 'would', 'could', 'should', 'may', 'might', 'can', 'do', 'does', 'did', 'get', 'got', 'go', 'went', 'come', 'came', 'see', 'saw', 'know', 'knew', 'think', 'thought', 'say', 'said', 'tell', 'told', 'give', 'gave', 'take', 'took', 'make', 'made', 'find', 'found', 'want', 'wanted', 'use', 'used', 'work', 'worked', 'way', 'ways', 'man', 'men', 'woman', 'women', 'child', 'children', 'year', 'years', 'day', 'days', 'time', 'times', 'life', 'lives', 'world', 'country', 'state', 'city', 'place', 'home', 'house', 'number', 'part', 'right', 'left', 'hand', 'eye', 'face', 'fact', 'case', 'point', 'group', 'company', 'system', 'program', 'question', 'problem', 'service', 'information', 'government', 'business', 'school', 'university', 'student', 'teacher', 'family', 'friend', 'money', 'book', 'water', 'food', 'car', 'house', 'room', 'office', 'door', 'window', 'table', 'chair', 'bed', 'wall', 'floor', 'phone', 'computer', 'internet', 'website', 'email', 'news', 'paper', 'magazine', 'television', 'radio', 'music', 'movie', 'game', 'sport', 'team', 'player', 'win', 'lose', 'play', 'run', 'walk', 'talk', 'listen', 'read', 'write', 'learn', 'teach', 'study', 'help', 'try', 'start', 'stop', 'end', 'begin', 'finish', 'continue', 'change', 'move', 'turn', 'open', 'close', 'show', 'hide', 'buy', 'sell', 'pay', 'cost', 'price', 'free', 'cheap', 'expensive', 'good', 'bad', 'best', 'worst', 'better', 'worse', 'great', 'small', 'big', 'large', 'little', 'long', 'short', 'high', 'low', 'old', 'new', 'young', 'early', 'late', 'fast', 'slow', 'easy', 'hard', 'difficult', 'simple', 'important', 'different', 'same', 'other', 'another', 'each', 'every', 'all', 'some', 'many', 'few', 'most', 'more', 'less', 'much', 'little', 'only', 'also', 'even', 'still', 'just', 'now', 'then', 'here', 'there', 'where', 'when', 'why', 'how', 'what', 'who', 'which', 'whose', 'whom', 'if', 'whether', 'because', 'since', 'although', 'though', 'while', 'until', 'unless', 'before', 'after', 'during', 'between', 'among', 'through', 'across', 'over', 'under', 'above', 'below', 'beside', 'behind', 'front', 'back', 'inside', 'outside', 'around', 'near', 'far', 'away', 'up', 'down', 'out', 'off', 'into', 'onto', 'upon', 'within', 'without', 'against', 'towards', 'toward', 'across', 'along', 'around', 'through', 'during', 'before', 'after', 'above', 'below', 'between', 'among', 'since', 'until', 'while', 'because', 'although', 'though', 'unless', 'except', 'beside', 'besides', 'despite', 'concerning', 'regarding', 'including', 'excluding', 'following', 'considering', 'according', 'depending', 'regardless'}

FR_STOP_WORDS = {'le', 'de', 'et', 'à', 'un', 'il', 'être', 'et', 'en', 'avoir', 'que', 'pour', 'dans', 'ce', 'son', 'une', 'sur', 'avec', 'ne', 'se', 'pas', 'tout', 'plus', 'par', 'grand', 'en', 'me', 'bien', 'où', 'ou', 'si', 'les', 'ce', 'mais', 'comme', 'dire', 'elle', 'deux', 'aussi', 'leur', 'voir', 'lui', 'nous', 'année', 'bon', 'jour', 'fois', 'très', 'faire', 'état', 'aller', 'enfant', 'venir', 'sans', 'maison', 'après', 'arriver', 'tant', 'donner', 'lieu', 'fin', 'pourquoi', 'aimer', 'heure', 'rester', 'savoir', 'falloir', 'debout', 'ici', 'cela', 'droit', 'pendant', 'matin', 'trop', 'dire', 'vouloir', 'tête', 'servir', 'bonne', 'depuis', 'demander', 'sec', 'rendre', 'compte', 'dès', 'attention', 'porte', 'loin', 'mer', 'bureau', 'parti', 'prendre', 'garde', 'place', 'main', 'grande', 'travailler', 'premier', 'monde', 'jour', 'jouer', 'part', 'temps', 'vie', 'cas', 'train', 'mari', 'route', 'esprit', 'église', 'fin', 'entrée', 'face', 'groupe', 'vers', 'politique', 'guerre', 'suivre', 'mettre', 'ordre', 'laisser', 'point', 'appeler', 'naître', 'mois', 'passer', 'peu', 'lequel', 'retour', 'fille', 'cour', 'sûr', 'compter', 'rôle', 'crier', 'équipe', 'façon', 'écouter', 'plan', 'soir', 'fond', 'millier', 'scale', 'fils', 'poser', 'écrire', 'long', 'ouvrir', 'noir', 'paix', 'forme', 'note', 'cours', 'image', 'amour', 'coup', 'durer', 'retourner', 'carte', 'école', 'sauf', 'mercure', 'droit', 'élever', 'victor', 'étude', 'second', 'langue', 'étape'}

class AdvancedNLPAnalyzer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.setup_models()
        self.setup_patterns()
        self.setup_trade_lexicon()
        
    def setup_models(self):
        """Initialise les modèles d'analyse (VERSION SIMPLIFIÉE)"""
        try:
            # VADER pour l'analyse de sentiment (optimisé pour les réseaux sociaux)
            self.vader_analyzer = SentimentIntensityAnalyzer()
            
            # Pas de transformers dans cette version
            self.transformer_sentiment = None
            
            # Pas de spaCy dans cette version
            self.nlp_en = None
            self.nlp_fr = None
            
            self.logger.info("NLP models initialized (simplified version)")
            
        except Exception as e:
            self.logger.error(f"Error initializing NLP models: {e}")
            raise

    def setup_patterns(self):
        """Configure les patterns de reconnaissance"""
        # Patterns pour détecter l'urgence/crise
        self.urgency_patterns = [
            r'\b(breaking|urgent|alert|emergency|crisis)\b',
            r'\b(immediate|critical|severe|massive)\b',
            r'\b(collapse|crash|plunge|surge|spike)\b',
            r'[🚨⚡🔥💥⛔🔴⚠️]+'
        ]
        
        # Patterns pour les montants et pourcentages
        self.financial_patterns = {
            'percentage': r'(\d+(?:\.\d+)?)\s*%',
            'currency': r'\$(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(billion|million|trillion)?',
            'stock_movement': r'\b(up|down|gained|lost|rose|fell|increased|decreased)\b'
        }
        
        # Mots d'intensité
        self.intensity_words = {
            'high': ['massive', 'huge', 'enormous', 'tremendous', 'catastrophic', 'devastating'],
            'medium': ['significant', 'notable', 'considerable', 'substantial', 'major'],
            'low': ['slight', 'minor', 'small', 'modest', 'limited']
        }

    def setup_trade_lexicon(self):
        """Configure le lexique spécialisé commerce international"""
        self.trade_sentiment_lexicon = {
            # Mots très positifs dans le contexte commercial
            'very_positive': {
                'breakthrough': 2.5, 'agreement': 2.0, 'deal': 1.8, 'cooperation': 1.7,
                'partnership': 1.6, 'resolution': 1.9, 'success': 1.8, 'progress': 1.5,
                'win-win': 2.2, 'beneficial': 1.6, 'opportunity': 1.4, 'growth': 1.5
            },
            # Mots très négatifs dans le contexte commercial
            'very_negative': {
                'war': -2.5, 'crisis': -2.3, 'collapse': -2.8, 'failure': -2.1,
                'retaliation': -2.0, 'sanctions': -1.9, 'embargo': -2.2, 'tariffs': -1.5,
                'escalation': -2.1, 'breakdown': -2.0, 'deadlock': -1.8, 'tensions': -1.6,
                'disruption': -1.7, 'uncertainty': -1.4, 'volatility': -1.3
            },
            # Indicateurs de volume/intensité
            'volume_indicators': {
                'massive': 1.5, 'huge': 1.4, 'major': 1.3, 'significant': 1.2,
                'minor': 0.8, 'slight': 0.7, 'limited': 0.6
            }
        }

    def detect_language(self, text: str) -> str:
        """Détecte la langue du texte"""
        # Mots français communs
        french_words = {'le', 'la', 'les', 'de', 'du', 'des', 'et', 'est', 'une', 'un', 'dans', 'sur', 'avec', 'pour'}
        # Mots anglais communs
        english_words = {'the', 'and', 'is', 'are', 'of', 'to', 'in', 'on', 'with', 'for', 'at', 'by'}
        
        words = set(text.lower().split())
        
        french_score = len(words & french_words)
        english_score = len(words & english_words)
        
        if french_score > english_score:
            return 'fr'
        return 'en'

    def preprocess_text(self, text: str, language: str = 'en') -> Dict[str, any]:
        """Préprocesse le texte et extrait des features (VERSION SIMPLIFIÉE)"""
        try:
            # Nettoyage de base
            original_text = text
            cleaned_text = self.clean_text(text)
            
            # Choisir les stop words selon la langue
            if language == 'fr':
                stop_words = FR_STOP_WORDS
            else:
                stop_words = EN_STOP_WORDS
            
            # Extraction des features (sans spaCy)
            features = {
                'original_text': original_text,
                'cleaned_text': cleaned_text,
                'language': language,
                'length': len(original_text),
                'word_count': len(cleaned_text.split()),
                'char_count': len(cleaned_text),
                'sentence_count': max(len(cleaned_text.split('.')), 1),
                'readability_score': self.calculate_readability(cleaned_text),
                'urgency_score': self.calculate_urgency_score(original_text),
                'financial_entities': self.extract_financial_entities(original_text),
                'entities': self.extract_entities_simple(original_text),  # Version simplifiée
                'keywords': self.extract_keywords(cleaned_text, stop_words),
                'hashtags': self.extract_hashtags(original_text),
                'mentions': self.extract_mentions(original_text),
                'urls': self.extract_urls(original_text)
            }
            
            return features
            
        except Exception as e:
            self.logger.error(f"Error in text preprocessing: {e}")
            return {
                'original_text': text,
                'cleaned_text': text,
                'language': language,
                'error': str(e)
            }

    def extract_entities_simple(self, text: str) -> Dict[str, List]:
        """Extraction d'entités simplifiée (sans spaCy)"""
        entities = {
            'persons': [],
            'organizations': [],
            'locations': [],
            'money': [],
            'dates': []
        }
        
        try:
            # Recherche d'entreprises connues
            companies = ['Apple', 'Microsoft', 'Google', 'Amazon', 'Tesla', 'Meta', 'NVIDIA', 'Intel', 'AMD', 'Qualcomm', 'Huawei', 'Samsung', 'TSMC', 'Toyota', 'Ford', 'GM', 'BMW', 'Mercedes', 'Volkswagen', 'Honda', 'Nissan', 'Hyundai', 'BYD', 'NIO', 'Caterpillar', 'GE', 'Siemens', 'Bosch', 'ABB', 'Honeywell', 'ExxonMobil', 'Shell', 'BP', 'Chevron', 'TotalEnergies', 'ConocoPhillips', 'Sinopec', 'PetroChina', 'Gazprom', 'Saudi Aramco', 'Cargill', 'ADM', 'Tyson Foods', 'JBS', 'Nestle', 'Unilever', 'JPMorgan', 'Bank of America', 'Wells Fargo', 'Goldman Sachs', 'Morgan Stanley', 'Citi', 'HSBC', 'Deutsche Bank', 'Credit Suisse']
            
            for company in companies:
                if company in text:
                    entities['organizations'].append(company)
            
            # Recherche de pays
            countries = ['USA', 'United States', 'China', 'Germany', 'Japan', 'France', 'UK', 'United Kingdom', 'Canada', 'Mexico', 'Brazil', 'India', 'Australia', 'South Korea', 'Italy', 'Spain', 'Netherlands', 'Switzerland', 'Singapore', 'Hong Kong', 'Taiwan', 'Russia', 'South Africa']
            
            for country in countries:
                if country in text:
                    entities['locations'].append(country)
            
            # Recherche de montants
            import re
            money_pattern = r'\$\d+(?:,\d{3})*(?:\.\d{2})?(?:\s*(?:billion|million|trillion))?'
            money_matches = re.findall(money_pattern, text, re.IGNORECASE)
            entities['money'] = money_matches[:5]  # Limiter à 5
            
        except Exception as e:
            self.logger.error(f"Error extracting entities: {e}")
        
        return entities

    def clean_text(self, text: str) -> str:
        """Nettoie le texte pour l'analyse"""
        if not text:
            return ""
            
        # Supprimer les URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Garder les hashtags et mentions mais les nettoyer
        text = re.sub(r'#(\w+)', r'\1', text)  # Garder le mot du hashtag
        text = re.sub(r'@(\w+)', r'', text)    # Supprimer les mentions
        
        # Nettoyer les caractères spéciaux mais garder la ponctuation importante
        text = re.sub(r'[^\w\s\.\!\?\,\-\%\$]', ' ', text)
        
        # Normaliser les espaces
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def calculate_readability(self, text: str) -> float:
        """Calcule le score de lisibilité"""
        try:
            if not text or len(text.strip()) == 0:
                return 50.0
            return textstat.flesch_reading_ease(text)
        except:
            return 50.0  # Score neutre par défaut

    def calculate_urgency_score(self, text: str) -> float:
        """Calcule un score d'urgence basé sur les patterns"""
        if not text:
            return 0.0
            
        urgency_score = 0.0
        text_lower = text.lower()
        
        for pattern in self.urgency_patterns:
            matches = len(re.findall(pattern, text_lower, re.IGNORECASE))
            urgency_score += matches * 0.3
        
        # Bonus pour les majuscules (indication d'urgence)
        caps_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
        urgency_score += caps_ratio * 0.5
        
        # Bonus pour les points d'exclamation
        exclamation_count = text.count('!')
        urgency_score += exclamation_count * 0.2
        
        return min(urgency_score, 2.0)  # Limité à 2.0

    def extract_financial_entities(self, text: str) -> Dict[str, List]:
        """Extrait les entités financières"""
        entities = {
            'percentages': [],
            'currencies': [],
            'stock_movements': []
        }
        
        if not text:
            return entities
        
        try:
            # Extraction des pourcentages
            percentages = re.findall(self.financial_patterns['percentage'], text)
            entities['percentages'] = [float(p) for p in percentages if p]
            
            # Extraction des montants en devises
            currency_matches = re.findall(self.financial_patterns['currency'], text)
            entities['currencies'] = currency_matches
            
            # Extraction des mouvements d'actions
            movements = re.findall(self.financial_patterns['stock_movement'], text, re.IGNORECASE)
            entities['stock_movements'] = movements
        except Exception as e:
            self.logger.error(f"Error extracting financial entities: {e}")
        
        return entities

    def extract_keywords(self, text: str, stop_words: set) -> List[str]:
        """Extrait les mots-clés importants"""
        if not text:
            return []
            
        try:
            words = text.lower().split()
            
            # Filtrer les stop words et les mots trop courts
            keywords = [
                word for word in words 
                if len(word) > 3 
                and word not in stop_words 
                and word.isalpha()
            ]
            
            # Retourner les mots les plus fréquents (max 10)
            word_freq = Counter(keywords)
            return [word for word, count in word_freq.most_common(10)]
        except Exception as e:
            self.logger.error(f"Error extracting keywords: {e}")
            return []

    def extract_hashtags(self, text: str) -> List[str]:
        """Extrait les hashtags"""
        if not text:
            return []
        try:
            return re.findall(r'#(\w+)', text)
        except:
            return []

    def extract_mentions(self, text: str) -> List[str]:
        """Extrait les mentions d'utilisateurs"""
        if not text:
            return []
        try:
            return re.findall(r'@(\w+)', text)
        except:
            return []

    def extract_urls(self, text: str) -> List[str]:
        """Extrait les URLs"""
        if not text:
            return []
        try:
            return re.findall(r'http\S+|www\S+|https\S+', text)
        except:
            return []

    def analyze_sentiment_vader(self, text: str) -> Dict[str, float]:
        """Analyse de sentiment avec VADER"""
        try:
            if not text:
                return self.get_default_sentiment()
                
            scores = self.vader_analyzer.polarity_scores(text)
            
            # Classification basée sur le score composé
            compound = scores['compound']
            if compound >= 0.05:
                label = 'positive'
            elif compound <= -0.05:
                label = 'negative'
            else:
                label = 'neutral'
            
            return {
                'method': 'vader',
                'label': label,
                'score': compound,
                'confidence': abs(compound),
                'details': {
                    'positive': scores['pos'],
                    'neutral': scores['neu'],
                    'negative': scores['neg']
                }
            }
        except Exception as e:
            self.logger.error(f"VADER sentiment analysis error: {e}")
            return self.get_default_sentiment()

    def analyze_sentiment_trade_context(self, text: str, features: Dict) -> Dict[str, float]:
        """Analyse de sentiment spécialisée pour le contexte commercial"""
        try:
            if not text:
                return self.get_default_sentiment()
                
            score = 0.0
            word_count = 0
            
            text_lower = text.lower()
            words = text_lower.split()
            
            # Analyser avec le lexique spécialisé
            for category, word_scores in self.trade_sentiment_lexicon.items():
                if category in ['very_positive', 'very_negative']:
                    for word, weight in word_scores.items():
                        if word in text_lower:
                            score += weight
                            word_count += 1
            
            # Ajuster selon l'urgence
            urgency_score = features.get('urgency_score', 0) if features else 0
            if urgency_score > 1.0:
                score = score * 1.3  # Amplifier le sentiment en cas d'urgence
            
            # Ajuster selon les entités financières
            financial_entities = features.get('financial_entities', {}) if features else {}
            percentages = financial_entities.get('percentages', [])
            movements = financial_entities.get('stock_movements', [])
            
            # Si de gros pourcentages sont mentionnés, amplifier
            if percentages:
                max_percentage = max(percentages)
                if max_percentage > 10:
                    score = score * (1 + max_percentage / 100)
            
            # Ajuster selon les mouvements d'actions
            positive_movements = ['up', 'gained', 'rose', 'increased']
            negative_movements = ['down', 'lost', 'fell', 'decreased']
            
            for movement in movements:
                if movement.lower() in positive_movements:
                    score += 0.5
                elif movement.lower() in negative_movements:
                    score -= 0.5
            
            # Normalisation finale
            normalized_score = max(-1.0, min(1.0, score / max(word_count, 1)))
            
            # Classification
            if normalized_score >= 0.1:
                label = 'positive'
            elif normalized_score <= -0.1:
                label = 'negative'
            else:
                label = 'neutral'
            
            return {
                'method': 'trade_context',
                'label': label,
                'score': normalized_score,
                'confidence': min(abs(normalized_score) + 0.3, 1.0),
                'details': {
                    'raw_score': score,
                    'word_count': word_count,
                    'urgency_boost': urgency_score > 1.0
                }
            }
        except Exception as e:
            self.logger.error(f"Trade context sentiment analysis error: {e}")
            return self.get_default_sentiment()

    def get_default_sentiment(self) -> Dict[str, float]:
        """Retourne un sentiment par défaut en cas d'erreur"""
        return {
            'method': 'default',
            'label': 'neutral',
            'score': 0.0,
            'confidence': 0.0,
            'details': {}
        }

    def ensemble_sentiment_analysis(self, text: str, features: Dict) -> Dict[str, any]:
        """Combine VADER + Trade context (sans transformers)"""
        try:
            if not text:
                return {
                    'label': 'neutral',
                    'score': 0.0,
                    'confidence': 0.0,
                    'error': 'Empty text'
                }
                
            # Analyser avec VADER et Trade context seulement
            vader_result = self.analyze_sentiment_vader(text)
            trade_result = self.analyze_sentiment_trade_context(text, features)
            
            # Pondération des méthodes (pas de transformer)
            weights = {
                'vader': 0.6,        # Plus de poids à VADER
                'trade_context': 0.4  # Spécialisé pour notre domaine
            }
            
            # Calcul du score pondéré
            weighted_score = (
                vader_result['score'] * weights['vader'] +
                trade_result['score'] * weights['trade_context']
            )
            
            # Calcul de la confiance pondérée
            weighted_confidence = (
                vader_result['confidence'] * weights['vader'] +
                trade_result['confidence'] * weights['trade_context']
            )
            
            # Classification finale
            if weighted_score >= 0.1:
                final_label = 'positive'
            elif weighted_score <= -0.1:
                final_label = 'negative'
            else:
                final_label = 'neutral'
            
            # Ajustement de la confiance selon la cohérence des méthodes
            methods_agreement = self.calculate_methods_agreement(
                [vader_result, trade_result]
            )
            
            final_confidence = weighted_confidence * methods_agreement
            
            return {
                'label': final_label,
                'score': weighted_score,
                'confidence': final_confidence,
                'methods': {
                    'vader': vader_result,
                    'trade_context': trade_result
                },
                'agreement_score': methods_agreement,
                'features_used': features
            }
            
        except Exception as e:
            self.logger.error(f"Ensemble sentiment analysis error: {e}")
            return {
                'label': 'neutral',
                'score': 0.0,
                'confidence': 0.0,
                'error': str(e)
            }

    def calculate_methods_agreement(self, results: List[Dict]) -> float:
        """Calcule l'accord entre les différentes méthodes"""
        try:
            labels = [r['label'] for r in results]
            scores = [r['score'] for r in results]
            
            # Accord sur les labels
            label_agreement = len(set(labels)) == 1
            
            # Accord sur les scores (écart-type faible = bon accord)
            score_std = np.std(scores)
            score_agreement = 1.0 / (1.0 + score_std)
            
            # Moyenne pondérée
            total_agreement = 0.6 * (1.0 if label_agreement else 0.5) + 0.4 * score_agreement
            
            return total_agreement
        except:
            return 0.5  # Valeur par défaut

    def analyze_text(self, text: str, language: str = None) -> Dict[str, any]:
        """Méthode principale d'analyse de texte (VERSION SIMPLIFIÉE)"""
        try:
            if not text:
                return {
                    'timestamp': datetime.now().isoformat(),
                    'original_text': '',
                    'error': 'Empty text',
                    'sentiment': self.get_default_sentiment()
                }
            
            # Détection de langue si non spécifiée
            if not language:
                language = self.detect_language(text)
            
            # Préprocessing
            features = self.preprocess_text(text, language)
            
            # Analyse de sentiment (sans transformers)
            sentiment_analysis = self.ensemble_sentiment_analysis(text, features)
            
            # Résultat final
            return {
                'timestamp': datetime.now().isoformat(),
                'original_text': text,
                'language': language,
                'features': features,
                'sentiment': sentiment_analysis,
                'processing_info': {
                    'analyzer_version': '1.0-simplified',
                    'models_used': ['vader', 'trade_lexicon'],
                    'processing_time': None  # À calculer si nécessaire
                }
            }
            
        except Exception as e:
            self.logger.error(f"Text analysis error: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'original_text': text,
                'error': str(e),
                'sentiment': self.get_default_sentiment()
            }