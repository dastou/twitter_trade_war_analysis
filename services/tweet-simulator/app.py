import os
import json
import time
import signal
import sys
from datetime import datetime
import logging
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
import threading
from tweet_generator import RealisticTweetGenerator

class TweetSimulatorApp:
    def __init__(self):
        self.setup_logging()
        self.load_config()
        self.setup_kafka_producer()
        self.tweet_generator = RealisticTweetGenerator()
        self.running = False
        self.config_refresh_interval = 120  # Refresh config every 2 minutes
        self.last_config_check = datetime.now()
        self.stats = {
            "tweets_sent": 0,
            "errors": 0,
            "start_time": None,
            "last_tweet_time": None,
            "config_refreshes": 0
        }
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def setup_logging(self):
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)

    def load_config(self):
        old_config = getattr(self, 'config', {})
        
        self.config = {
            'kafka_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'topic_name': os.getenv('KAFKA_TOPIC_TWEETS', 'raw-tweets'),
            'tweet_rate': int(os.getenv('TWEET_RATE', '10')),
            'batch_size': int(os.getenv('BATCH_SIZE', '1')),
            'enable_monitoring': os.getenv('ENABLE_MONITORING', 'true').lower() == 'true',
            'simulation_mode': os.getenv('SIMULATION_MODE', 'normal'),
            'language_mix': os.getenv('LANGUAGE_MIX', 'en:0.7,fr:0.3')
        }
        
        if old_config:
            changes = []
            for key, new_value in self.config.items():
                old_value = old_config.get(key)
                if old_value != new_value:
                    changes.append(f"{key}: {old_value} -> {new_value}")
            
            if changes:
                self.logger.info(f"Configuration updated: {', '.join(changes)}")
                self.stats["config_refreshes"] += 1
                
                if hasattr(self, 'tweet_generator'):
                    self.apply_config_changes(old_config)
        
        self.logger.info(f"Configuration loaded: {self.config}")

    def apply_config_changes(self, old_config):
        try:
            if old_config.get('simulation_mode') != self.config['simulation_mode']:
                new_scenario = self.config['simulation_mode']
                if new_scenario in self.tweet_generator.scenarios:
                    old_scenario = self.tweet_generator.current_scenario
                    self.tweet_generator.current_scenario = new_scenario
                    self.tweet_generator.scenario_start_time = datetime.now()
                    self.logger.info(f"Scenario changed: {old_scenario} -> {new_scenario}")
                else:
                    self.logger.warning(f"Unknown scenario: {new_scenario}, keeping current: {self.tweet_generator.current_scenario}")
            
            if old_config.get('tweet_rate') != self.config['tweet_rate']:
                self.logger.info(f"Tweet rate changed: {old_config.get('tweet_rate')} -> {self.config['tweet_rate']} tweets/min")
        
        except Exception as e:
            self.logger.error(f"Error applying config changes: {e}")

    def refresh_config_if_needed(self):
        now = datetime.now()
        if (now - self.last_config_check).total_seconds() >= self.config_refresh_interval:
            self.load_config()
            self.last_config_check = now

    def setup_kafka_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['kafka_servers'].split(','),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=1000,
                request_timeout_ms=30000,
                compression_type='gzip',
                batch_size=16384,
                linger_ms=100,
                buffer_memory=33554432
            )
            self.logger.info("Kafka producer initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            sys.exit(1)

    def wait_for_kafka(self):
        max_retries = 30
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                metadata = self.producer.bootstrap_connected()
                if metadata:
                    self.logger.info("Successfully connected to Kafka")
                    return True
                    
            except Exception as e:
                self.logger.warning(f"Kafka connection attempt {retry_count + 1}/{max_retries} failed: {e}")
                
            retry_count += 1
            time.sleep(2)
        
        self.logger.error("Failed to connect to Kafka after maximum retries")
        return False

    def send_tweet(self, tweet: Dict[str, Any]) -> bool:
        try:
            key = tweet.get('tweet_id', str(self.stats['tweets_sent']))
            
            tweet_with_metadata = {
                **tweet,
                'producer_timestamp': datetime.now().isoformat(),
                'producer_id': 'tweet-simulator',
                'version': '1.0',
                'config_scenario': self.config['simulation_mode'],
                'config_rate': self.config['tweet_rate']
            }
            
            future = self.producer.send(
                self.config['topic_name'],
                value=tweet_with_metadata,
                key=key
            )
            
            record_metadata = future.get(timeout=10)
            
            self.stats['tweets_sent'] += 1
            self.stats['last_tweet_time'] = datetime.now()
            
            if self.stats['tweets_sent'] % 50 == 0:
                self.logger.info(f"Sent {self.stats['tweets_sent']} tweets. "
                               f"Last sent to partition {record_metadata.partition}")
            
            return True
            
        except KafkaError as e:
            self.logger.error(f"Kafka error sending tweet: {e}")
            self.stats['errors'] += 1
            return False
            
        except Exception as e:
            self.logger.error(f"Unexpected error sending tweet: {e}")
            self.stats['errors'] += 1
            return False

    def calculate_dynamic_delay(self) -> float:
        base_delay = 60.0 / max(self.config['tweet_rate'], 1)
        
        volume_factor = self.tweet_generator.get_current_volume_factor()
        weekend_factor = self.tweet_generator.adjust_for_weekend()
        
        delay = base_delay / (volume_factor * weekend_factor)
        
        import random
        noise = random.gauss(1.0, 0.3)
        final_delay = max(0.5, delay * noise)
        
        return final_delay

    def generate_and_send_tweets(self):
        self.logger.info("Starting tweet generation loop")
        
        while self.running:
            try:
                self.refresh_config_if_needed()
                
                tweet = self.tweet_generator.generate_tweet()
                
                if tweet.get('is_anomaly', False):
                    self.logger.warning(f"Anomaly detected: {tweet['scenario']} - {tweet['text'][:100]}...")
                
                if hasattr(self, '_last_logged_scenario'):
                    if self._last_logged_scenario != tweet['scenario']:
                        self.logger.info(f"Scenario transition: {self._last_logged_scenario} -> {tweet['scenario']}")
                
                self._last_logged_scenario = tweet['scenario']
                
                success = self.send_tweet(tweet)
                
                if not success:
                    self.logger.warning("Failed to send tweet, continuing...")
                
                delay = self.calculate_dynamic_delay()
                
                if self.stats['tweets_sent'] % 25 == 0 and self.stats['tweets_sent'] > 0:
                    self.log_statistics()
                
                time.sleep(delay)
                
            except KeyboardInterrupt:
                self.logger.info("Received keyboard interrupt")
                break
                
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                self.stats['errors'] += 1
                time.sleep(5)

    def log_statistics(self):
        scenario_stats = self.tweet_generator.get_scenario_stats()
        uptime = datetime.now() - self.stats['start_time']
        
        stats_message = (
            f"Stats - Tweets: {self.stats['tweets_sent']}, "
            f"Errors: {self.stats['errors']}, "
            f"Rate: {self.config['tweet_rate']}/min, "
            f"Uptime: {uptime}, "
            f"Scenario: {scenario_stats['current_scenario']}, "
            f"Volume factor: {scenario_stats['volume_factor']:.2f}, "
            f"Config refreshes: {self.stats['config_refreshes']}"
        )
        
        self.logger.info(stats_message)

    def signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()

    def start(self):
        self.logger.info("Starting Tweet Simulator")
        
        if not self.wait_for_kafka():
            self.logger.error("Cannot start without Kafka connection")
            sys.exit(1)
        
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        self.logger.info(f"Tweet generation started - Rate: {self.config['tweet_rate']} tweets/min, "
                        f"Scenario: {self.config['simulation_mode']}, "
                        f"Config refresh: every {self.config_refresh_interval//60} minutes")
        
        if self.config['simulation_mode'] in self.tweet_generator.scenarios:
            self.tweet_generator.current_scenario = self.config['simulation_mode']
            self.tweet_generator.scenario_start_time = datetime.now()
            self.logger.info(f"Initial scenario set to: {self.config['simulation_mode']}")
        
        self.generate_and_send_tweets()

    def stop(self):
        self.logger.info("Stopping Tweet Simulator...")
        self.running = False
        
        if hasattr(self, 'producer'):
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=10)
                self.logger.info("Kafka producer closed")
            except Exception as e:
                self.logger.error(f"Error closing Kafka producer: {e}")
        
        if self.stats['start_time']:
            final_uptime = datetime.now() - self.stats['start_time']
            self.logger.info(f"Final stats - Tweets sent: {self.stats['tweets_sent']}, "
                           f"Errors: {self.stats['errors']}, "
                           f"Config refreshes: {self.stats['config_refreshes']}, "
                           f"Total uptime: {final_uptime}")

if __name__ == "__main__":
    app = TweetSimulatorApp()
    try:
        app.start()
    except Exception as e:
        app.logger.error(f"Application crashed: {e}")
        app.stop()
        sys.exit(1)