import json
import random
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
from typing import Dict, List, Tuple
import logging

class RealisticTweetGenerator:
    def __init__(self, initial_scenario="normal"):
        self.fake = Faker(['en_US', 'fr_FR'])
        self.load_data()
        self.setup_scenarios()
        self.current_scenario = initial_scenario
        self.scenario_start_time = datetime.now()
        self.tweet_counter = 0
        self.scenario_lock_duration = None
        
        self.hourly_patterns = self._generate_hourly_patterns()
        self.daily_volatility = 0.3
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        if initial_scenario not in self.scenarios:
            self.logger.warning(f"Unknown initial scenario '{initial_scenario}', defaulting to 'normal'")
            self.current_scenario = "normal"

    def load_data(self):
        with open('/app/data/keywords.json', 'r') as f:
            self.keywords = json.load(f)
        
        with open('/app/data/companies.json', 'r') as f:
            self.companies = json.load(f)
            
        with open('/app/data/tweet_templates.json', 'r') as f:
            self.templates = json.load(f)

    def setup_scenarios(self):
        self.scenarios = {
            "normal": {
                "sentiment_distribution": {"positive": 0.3, "neutral": 0.4, "negative": 0.3},
                "volume_multiplier": 1.0,
                "crisis_probability": 0.02,
                "anomaly_probability": 0.05
            },
            "trade_war_escalation": {
                "sentiment_distribution": {"positive": 0.1, "neutral": 0.2, "negative": 0.7},
                "volume_multiplier": 3.5,
                "crisis_probability": 0.15,
                "anomaly_probability": 0.25
            },
            "breakthrough_deal": {
                "sentiment_distribution": {"positive": 0.7, "neutral": 0.2, "negative": 0.1},
                "volume_multiplier": 2.8,
                "crisis_probability": 0.01,
                "anomaly_probability": 0.20
            },
            "supply_chain_crisis": {
                "sentiment_distribution": {"positive": 0.15, "neutral": 0.25, "negative": 0.6},
                "volume_multiplier": 2.2,
                "crisis_probability": 0.20,
                "anomaly_probability": 0.30
            },
            "market_uncertainty": {
                "sentiment_distribution": {"positive": 0.25, "neutral": 0.3, "negative": 0.45},
                "volume_multiplier": 1.5,
                "crisis_probability": 0.08,
                "anomaly_probability": 0.15
            },
            "realistic": {
                "sentiment_distribution": {"positive": 0.3, "neutral": 0.4, "negative": 0.3},
                "volume_multiplier": 1.0,
                "crisis_probability": 0.02,
                "anomaly_probability": 0.05
            }
        }

    def force_scenario(self, scenario: str, duration_minutes: int = None):
        if scenario not in self.scenarios:
            self.logger.error(f"Unknown scenario: {scenario}")
            return False
        
        old_scenario = self.current_scenario
        self.current_scenario = scenario
        self.scenario_start_time = datetime.now()
        
        if duration_minutes:
            self.scenario_lock_duration = duration_minutes
            self.logger.info(f"Forced scenario '{scenario}' for {duration_minutes} minutes (was '{old_scenario}')")
        else:
            self.scenario_lock_duration = None
            self.logger.info(f"Scenario changed to '{scenario}' (was '{old_scenario}')")
        
        return True

    def is_scenario_locked(self) -> bool:
        if self.scenario_lock_duration is None:
            return False
        
        elapsed = (datetime.now() - self.scenario_start_time).total_seconds() / 60
        if elapsed >= self.scenario_lock_duration:
            self.scenario_lock_duration = None
            self.logger.info(f"Scenario lock expired, scenario '{self.current_scenario}' is now unlocked")
            return False
        
        return True

    def _generate_hourly_patterns(self) -> List[float]:
        base_pattern = [
            0.3, 0.2, 0.15, 0.1, 0.1, 0.15,
            0.25, 0.4, 0.6, 0.8, 1.0, 1.2,
            1.3, 1.4, 1.2, 1.0, 0.9, 0.8,
            0.7, 0.6, 0.5, 0.4, 0.35, 0.3
        ]
        return base_pattern

    def should_change_scenario(self) -> bool:
        if self.is_scenario_locked():
            return False
        
        time_in_scenario = datetime.now() - self.scenario_start_time
        
        if self.current_scenario == "normal":
            return time_in_scenario > timedelta(hours=random.uniform(2, 6))
        else:
            return time_in_scenario > timedelta(minutes=random.uniform(30, 120))

    def select_new_scenario(self) -> str:
        if self.is_scenario_locked():
            return self.current_scenario
        
        if self.current_scenario != "normal":
            if random.random() < 0.7:
                return "normal"
        
        scenario_weights = {
            "normal": 0.4,
            "trade_war_escalation": 0.2,
            "breakthrough_deal": 0.15,
            "supply_chain_crisis": 0.15,
            "market_uncertainty": 0.1
        }
        
        return random.choices(
            list(scenario_weights.keys()),
            weights=list(scenario_weights.values())
        )[0]

    def get_current_volume_factor(self) -> float:
        current_hour = datetime.now().hour
        hourly_factor = self.hourly_patterns[current_hour]
        scenario_factor = self.scenarios[self.current_scenario]["volume_multiplier"]
        
        noise = random.gauss(1.0, self.daily_volatility)
        
        return hourly_factor * scenario_factor * max(0.1, noise)

    def generate_realistic_engagement(self, sentiment: str, is_crisis: bool = False) -> Dict:
        base_engagement = {
            "positive": {"likes": (5, 50), "retweets": (2, 25), "replies": (1, 15)},
            "neutral": {"likes": (2, 20), "retweets": (1, 10), "replies": (0, 8)},
            "negative": {"likes": (10, 80), "retweets": (5, 40), "replies": (3, 25)}
        }
        
        engagement = {}
        for metric, (min_val, max_val) in base_engagement[sentiment].items():
            if is_crisis:
                min_val *= 3
                max_val *= 5
            
            engagement[metric] = random.randint(min_val, max_val)
        
        return engagement

    def select_sentiment(self) -> str:
        distribution = self.scenarios[self.current_scenario]["sentiment_distribution"]
        return random.choices(
            list(distribution.keys()),
            weights=list(distribution.values())
        )[0]

    def generate_geographic_data(self) -> Dict:
        regions = {
            "North America": {"weight": 0.35, "countries": ["USA", "Canada", "Mexico"]},
            "Europe": {"weight": 0.25, "countries": ["Germany", "France", "UK", "Netherlands"]},
            "Asia Pacific": {"weight": 0.30, "countries": ["China", "Japan", "South Korea", "Singapore"]},
            "Other": {"weight": 0.10, "countries": ["Brazil", "India", "Australia", "South Africa"]}
        }
        
        region = random.choices(
            list(regions.keys()),
            weights=[r["weight"] for r in regions.values()]
        )[0]
        
        country = random.choice(regions[region]["countries"])
        coordinates = self._get_country_coordinates(country)
        
        return {
            "country": country,
            "region": region,
            "coordinates": coordinates
        }

    def _get_country_coordinates(self, country: str) -> List[float]:
        coords = {
            "USA": [39.8283, -98.5795], "Canada": [56.1304, -106.3468],
            "Mexico": [23.6345, -102.5528], "Germany": [51.1657, 10.4515],
            "France": [46.6034, 1.8883], "UK": [55.3781, -3.4360],
            "Netherlands": [52.1326, 5.2913], "China": [35.8617, 104.1954],
            "Japan": [36.2048, 138.2529], "South Korea": [35.9078, 127.7669],
            "Singapore": [1.3521, 103.8198], "Brazil": [-14.2350, -51.9253],
            "India": [20.5937, 78.9629], "Australia": [-25.2744, 133.7751],
            "South Africa": [-30.5595, 22.9375]
        }
        return coords.get(country, [0.0, 0.0])

    def create_crisis_tweet(self) -> Dict:
        crisis_events = [
            "Major shipping route blocked due to geopolitical tensions",
            "Critical semiconductor facility damaged in natural disaster",
            "Emergency trade talks collapse between major economies",
            "Cyber attack disrupts global supply chain systems",
            "Natural disaster affects key production hub",
            "Political instability in major trade partner escalates",
            "Currency manipulation allegations surface in G20 meeting",
            "Trade sanctions expanded to include technology sector",
            "WTO dispute resolution mechanism suspended indefinitely",
            "Strategic materials export ban triggers market panic"
        ]
        
        template = random.choice(self.templates["crisis_style"])
        event = random.choice(crisis_events)
        
        replacements = {
            "crisis_event": event,
            "shock_event": event,
            "supply_chain_component": random.choice(["shipping", "manufacturing", "logistics", "procurement"]),
            "companies": ", ".join(random.sample(self._get_all_companies(), 2)),
            "hashtags": " ".join(random.sample(self.keywords["hashtags"]["general"], 3)),
            "country": random.choice(["China", "USA", "Germany", "Japan", "EU"]),
            "target_country": random.choice(["USA", "EU", "China", "UK", "Japan"]),
            "percentage": str(random.randint(10, 50)),
            "direction": random.choice(["up", "down", "plummet", "surge"]),
            "commodity": random.choice(["oil", "steel", "semiconductors", "rare earths", "lithium"]),
            "product_category": random.choice(["tech", "automotive", "agricultural", "energy"]),
            "trade_action": random.choice(["imports", "exports", "investments", "joint ventures"]),
            "natural_disaster": random.choice(["typhoon", "earthquake", "flood", "wildfire"]),
            "political_event": random.choice(["sanctions", "policy change", "leadership change", "election"]),
            "trade_response": random.choice(["retaliation", "embargo", "restrictions", "countermeasures"])
        }
        
        tweet_text = self._replace_template_vars(template, replacements)
        
        return {
            "text": tweet_text,
            "sentiment": "negative",
            "is_crisis": True,
            "priority": "high"
        }

    def _get_all_companies(self) -> List[str]:
        all_companies = []
        for sector in self.companies.values():
            all_companies.extend(sector)
        return all_companies

    def _replace_template_vars(self, template: str, replacements: Dict) -> str:
        text = template
        for var, value in replacements.items():
            text = text.replace(f"{{{var}}}", str(value))
        
        remaining_vars = {
            "event": "market development",
            "company": random.choice(self._get_all_companies()),
            "direction": random.choice(["up", "down"]),
            "percentage": str(random.randint(1, 15)),
            "trade_topic": random.choice(self.keywords["trade_topics"]["tariffs"]),
            "sentiment_word": random.choice(["positively", "negatively", "mixed"]),
            "government": random.choice(["US", "EU", "China", "Japan"]),
            "trade_measure": random.choice(["tariffs", "sanctions", "restrictions"]),
            "sector": random.choice(["tech", "automotive", "energy", "agriculture"]),
            "country1": "USA", "country2": "China",
            "action": "new trade policy"
        }
        
        for var, value in remaining_vars.items():
            text = text.replace(f"{{{var}}}", str(value))
        
        return text

    def generate_tweet(self) -> Dict:
        if self.should_change_scenario():
            old_scenario = self.current_scenario
            self.current_scenario = self.select_new_scenario()
            self.scenario_start_time = datetime.now()
            self.logger.info(f"Scenario auto-changed: {old_scenario} -> {self.current_scenario}")

        is_crisis = random.random() < self.scenarios[self.current_scenario]["crisis_probability"]
        
        if is_crisis:
            tweet_data = self.create_crisis_tweet()
        else:
            sentiment = self.select_sentiment()
            template_category = random.choices(
                list(self.templates.keys()),
                weights=[3, 2, 2, 1, 1]
            )[0]
            
            template = random.choice(self.templates[template_category])
            tweet_text = self._generate_normal_tweet_content(template, sentiment)
            
            tweet_data = {
                "text": tweet_text,
                "sentiment": sentiment,
                "is_crisis": False,
                "priority": "normal"
            }

        self.tweet_counter += 1
        
        tweet = {
            "tweet_id": f"tweet_{self.tweet_counter}_{int(datetime.now().timestamp())}",
            "text": tweet_data["text"][:280],
            "timestamp": datetime.now().isoformat(),
            "user": f"user_{random.randint(1000, 9999)}",
            "language": random.choice(["en", "fr"]) if random.random() < 0.3 else "en",
            "sentiment": {
                "label": tweet_data["sentiment"],
                "score": self._calculate_sentiment_score(tweet_data["sentiment"]),
                "confidence": random.uniform(0.7, 0.95)
            },
            "engagement": self.generate_realistic_engagement(
                tweet_data["sentiment"], 
                tweet_data.get("is_crisis", False)
            ),
            "location": self.generate_geographic_data(),
            "keywords": self._extract_keywords(tweet_data["text"]),
            "entities": self._extract_entities(tweet_data["text"]),
            "is_anomaly": tweet_data.get("is_crisis", False),
            "scenario": self.current_scenario,
            "priority": tweet_data.get("priority", "normal"),
            "scenario_locked": self.is_scenario_locked()
        }

        return tweet

    def _generate_normal_tweet_content(self, template: str, sentiment: str) -> str:
        if sentiment == "positive":
            trigger_words = self.keywords["sentiment_triggers"]["positive"]
            direction = "up"
            opinion_word = random.choice(["excellent", "great", "promising", "beneficial"])
            emotion = random.choice(["optimistic", "confident", "excited"])
        elif sentiment == "negative":
            trigger_words = self.keywords["sentiment_triggers"]["negative"]
            direction = "down"
            opinion_word = random.choice(["terrible", "concerning", "disastrous", "harmful"])
            emotion = random.choice(["worried", "concerned", "frustrated"])
        else:
            trigger_words = self.keywords["sentiment_triggers"]["neutral"]
            direction = random.choice(["up", "down"])
            opinion_word = random.choice(["expected", "standard", "typical", "normal"])
            emotion = random.choice(["cautious", "analytical", "observant"])

        replacements = {
            "event": random.choice(trigger_words),
            "company": random.choice(self._get_all_companies()),
            "direction": direction,
            "percentage": str(random.randint(1, 15)),
            "trade_topic": random.choice(
                self.keywords["trade_topics"][random.choice(list(self.keywords["trade_topics"].keys()))]
            ),
            "sentiment_word": random.choice(["positively", "negatively", "cautiously"]),
            "sentiment_adjective": opinion_word,
            "government": random.choice(["US", "EU", "China", "Japan", "UK"]),
            "trade_measure": random.choice(["tariffs", "sanctions", "restrictions", "quotas"]),
            "sector": random.choice(["technology", "automotive", "energy", "agriculture", "manufacturing"]),
            "country1": random.choice(["USA", "China", "Germany", "Japan"]),
            "country2": random.choice(["China", "USA", "EU", "UK"]),
            "action": random.choice(["policy announcement", "trade restriction", "agreement signing"]),
            "hashtags": " ".join(random.sample(self.keywords["hashtags"]["general"], random.randint(2, 4))),
            "metric": random.choice(self.keywords["economic_indicators"]),
            "official": random.choice(["Trade Representative", "Commerce Secretary", "Finance Minister"]),
            "quote": self._generate_quote(sentiment),
            "trade_prediction": self._generate_prediction(sentiment),
            "opinion_word": opinion_word,
            "prediction": self._generate_company_prediction(sentiment),
            "consequence": self._generate_consequence(sentiment),
            "emotion": emotion,
            "impact_word": random.choice(["significant", "minimal", "moderate", "severe"]),
            "feeling": emotion,
            "trade_situation": random.choice(["current tensions", "ongoing negotiations", "trade dispute"]),
            "status": random.choice(["adapting", "struggling", "thriving", "repositioning"]),
            "impact_verb": random.choice(["boost", "harm", "affect", "transform"]),
            "companies": ", ".join(random.sample(self._get_all_companies(), 2)),
            "severity": random.choice(["limited", "moderate", "significant", "severe"]),
            "economic_indicator": random.choice(self.keywords["economic_indicators"]),
            "condition": "tariffs increase",
            "outcome": random.choice(["price volatility", "supply disruption", "market consolidation"]),
            "timeframe": random.choice(["this quarter", "next year", "short term"]),
            "outlook": random.choice(["positive", "negative", "uncertain", "stable"]),
            "risk_level": random.choice(["low", "medium", "high", "critical"]),
            "strategy": random.choice(["diversification", "cost reduction", "market expansion"]),
            "department": random.choice(["Commerce Dept", "Trade Office", "Treasury"]),
            "date": "immediately",
            "sectors": random.choice(["tech, automotive", "energy, manufacturing", "agriculture"]),
            "country": random.choice(["USA", "China", "Germany", "Japan"]),
            "trade_action": random.choice(["policy", "agreement", "restriction"]),
            "trade_agreement": random.choice(["bilateral", "multilateral", "sectoral"]),
            "agency": random.choice(["Commerce", "Treasury", "USTR"]),
            "trade_issue": random.choice(["dumping", "subsidies", "IP theft"]),
            "conclusion": random.choice(["violations found", "no issues", "further review needed"]),
            "trade_activity": random.choice(["investments", "acquisitions", "partnerships"]),
            "duration": random.choice(["90 days", "6 months", "1 year"])
        }

        return self._replace_template_vars(template, replacements)

    def _generate_quote(self, sentiment: str) -> str:
        quotes = {
            "positive": [
                "We are committed to fair and balanced trade",
                "This agreement benefits both nations",
                "Progress is being made on all fronts",
                "We look forward to continued cooperation"
            ],
            "negative": [
                "These actions are unacceptable",
                "We will respond appropriately",
                "This threatens global stability",
                "Immediate action is required"
            ],
            "neutral": [
                "We are monitoring the situation",
                "Discussions are ongoing",
                "All options remain on the table",
                "We will assess the impact"
            ]
        }
        return random.choice(quotes[sentiment])

    def _generate_prediction(self, sentiment: str) -> str:
        predictions = {
            "positive": [
                "trade tensions will ease within months",
                "both sides will reach a compromise",
                "markets will stabilize soon"
            ],
            "negative": [
                "tensions will escalate further",
                "trade war is inevitable",
                "economic damage will worsen"
            ],
            "neutral": [
                "negotiations will continue",
                "situation remains fluid",
                "outcomes are uncertain"
            ]
        }
        return random.choice(predictions[sentiment])

    def _generate_company_prediction(self, sentiment: str) -> str:
        predictions = {
            "positive": [
                "benefit from this development",
                "see increased market share",
                "expand operations"
            ],
            "negative": [
                "face significant challenges",
                "reduce production capacity",
                "restructure operations"
            ],
            "neutral": [
                "adapt to new conditions",
                "monitor developments",
                "maintain current strategy"
            ]
        }
        return random.choice(predictions[sentiment])

    def _generate_consequence(self, sentiment: str) -> str:
        consequences = {
            "positive": [
                "boost economic growth",
                "create new opportunities",
                "strengthen partnerships"
            ],
            "negative": [
                "disrupt supply chains",
                "increase consumer prices",
                "harm economic growth"
            ],
            "neutral": [
                "require market adjustments",
                "need careful monitoring",
                "create mixed outcomes"
            ]
        }
        return random.choice(consequences[sentiment])

    def _calculate_sentiment_score(self, sentiment: str) -> float:
        base_scores = {"positive": 0.7, "neutral": 0.0, "negative": -0.7}
        base = base_scores[sentiment]
        noise = random.gauss(0, 0.2)
        return max(-1.0, min(1.0, base + noise))

    def _extract_keywords(self, text: str) -> List[str]:
        found_keywords = []
        text_lower = text.lower()
        
        for category in self.keywords["trade_topics"].values():
            for keyword in category:
                if keyword.lower() in text_lower:
                    found_keywords.append(keyword)
        
        return found_keywords[:5]

    def _extract_entities(self, text: str) -> Dict:
        entities = {"companies": [], "countries": [], "persons": []}
        
        for company_list in self.companies.values():
            for company in company_list:
                if company in text:
                    entities["companies"].append(company)
        
        countries = ["USA", "China", "Germany", "Japan", "France", "UK", "Canada", "Mexico"]
        for country in countries:
            if country in text:
                entities["countries"].append(country)
        
        return entities

    def get_scenario_stats(self) -> Dict:
        return {
            "current_scenario": self.current_scenario,
            "scenario_duration": str(datetime.now() - self.scenario_start_time),
            "tweets_generated": self.tweet_counter,
            "volume_factor": self.get_current_volume_factor(),
            "scenario_locked": self.is_scenario_locked(),
            "lock_remaining": str(timedelta(minutes=self.scenario_lock_duration) - (datetime.now() - self.scenario_start_time)) if self.is_scenario_locked() else None
        }

    def simulate_market_hours_activity(self) -> bool:
        current_hour = datetime.now().hour
        
        peak_hours = [
            (8, 12),
            (13, 17),
            (22, 2)
        ]
        
        for start, end in peak_hours:
            if start <= end:
                if start <= current_hour <= end:
                    return True
            else:
                if current_hour >= start or current_hour <= end:
                    return True
        
        return False

    def adjust_for_weekend(self) -> float:
        weekday = datetime.now().weekday()
        if weekday >= 5:
            return 0.3
        return 1.0

    def should_generate_anomaly(self) -> bool:
        anomaly_prob = self.scenarios[self.current_scenario]["anomaly_probability"]
        return random.random() < anomaly_prob

    def get_next_tweet_delay(self) -> float:
        base_delay = 6.0
        volume_factor = self.get_current_volume_factor()
        weekend_factor = self.adjust_for_weekend()
        
        delay = base_delay / (volume_factor * weekend_factor)
        
        noise = random.gauss(1.0, 0.3)
        final_delay = max(1.0, delay * noise)
        
        return final_delay