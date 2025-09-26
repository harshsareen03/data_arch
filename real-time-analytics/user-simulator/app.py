import json
import time
import random
import logging
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

class EnhancedUserEventProducer:
    def __init__(self, kafka_brokers='kafka:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10
        )
        self.topics = ['user-events']
        
        # Enhanced data sets
        self.products = {
            'electronics': ['iPhone 15', 'MacBook Pro', 'iPad Air', 'AirPods', 'Apple Watch'],
            'clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Sneakers', 'Dress'],
            'books': ['Python Programming', 'Data Science', 'Machine Learning', 'Kubernetes Guide'],
            'home': ['Coffee Maker', 'Blender', 'Vacuum Cleaner', 'Air Fryer']
        }
        
        self.categories = list(self.products.keys())
        self.user_profiles = self.generate_user_profiles(1000)

    def generate_user_profiles(self, count):
        """Generate realistic user profiles with purchase behavior"""
        profiles = {}
        for i in range(1, count + 1):
            profiles[i] = {
                'user_id': i,
                'name': fake.name(),
                'email': fake.email(),
                'age': random.randint(18, 70),
                'preferred_category': random.choice(self.categories),
                'purchase_frequency': random.choice(['low', 'medium', 'high']),
                'avg_order_value': random.uniform(25, 500),
                'location': {
                    'country': fake.country(),
                    'city': fake.city(),
                    'timezone': fake.timezone()
                },
                'loyalty_tier': random.choice(['bronze', 'silver', 'gold', 'platinum']),
                'signup_date': fake.date_between(start_date='-2y', end_date='today')
            }
        return profiles

    def generate_user_event(self, user_id):
        """Generate enhanced user event data"""
        user_profile = self.user_profiles.get(user_id, self.user_profiles[random.randint(1, 1000)])
        
        event_types_weights = {
            'page_view': 0.4,
            'product_view': 0.2,
            'add_to_cart': 0.15,
            'purchase': 0.1,
            'login': 0.08,
            'logout': 0.05,
            'search': 0.02
        }
        
        event_type = random.choices(
            list(event_types_weights.keys()),
            weights=list(event_types_weights.values())
        )[0]
        
        # Generate event-specific data
        event_data = self.generate_event_specific_data(event_type, user_profile)
        
        event = {
            'event_id': str(uuid.uuid4()),
            'user_id': user_id,
            'user_profile': {
                'name': user_profile['name'],
                'email': user_profile['email'],
                'age': user_profile['age'],
                'loyalty_tier': user_profile['loyalty_tier'],
                'location': user_profile['location']
            },
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'session_id': f"session_{user_id}_{datetime.utcnow().strftime('%Y%m%d')}",
            'page_url': fake.url(),
            'user_agent': fake.user_agent(),
            'ip_address': fake.ipv4(),
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
            'operating_system': random.choice(['windows', 'macos', 'linux', 'ios', 'android']),
            'referrer': random.choice([fake.url(), '', 'google.com', 'facebook.com']),
            'utm_parameters': {
                'utm_source': random.choice(['google', 'facebook', 'email', 'direct']),
                'utm_medium': random.choice(['cpc', 'organic', 'social', 'email']),
                'utm_campaign': f"campaign_{random.randint(1, 20)}"
            },
            **event_data
        }
        
        return event

    def generate_event_specific_data(self, event_type, user_profile):
        """Generate data specific to each event type"""
        base_data = {}
        
        if event_type == 'purchase':
            category = user_profile['preferred_category']
            products = random.sample(self.products[category], random.randint(1, 3))
            
            base_data = {
                'products': products,
                'order_value': round(random.uniform(20, 500), 2),
                'payment_method': random.choice(['credit_card', 'paypal', 'apple_pay']),
                'shipping_address': {
                    'street': fake.street_address(),
                    'city': fake.city(),
                    'zipcode': fake.zipcode(),
                    'country': fake.country()
                },
                'order_id': f"ORD{random.randint(10000, 99999)}",
                'tax_amount': round(random.uniform(2, 50), 2),
                'shipping_cost': round(random.uniform(0, 20), 2)
            }
        elif event_type in ['product_view', 'add_to_cart']:
            category = random.choice(self.categories)
            base_data = {
                'product_id': f"PROD{random.randint(1000, 9999)}",
                'product_name': random.choice(self.products[category]),
                'product_category': category,
                'product_price': round(random.uniform(10, 1000), 2),
                'quantity': random.randint(1, 3) if event_type == 'add_to_cart' else 1
            }
        elif event_type == 'search':
            base_data = {
                'search_query': fake.words(nb=random.randint(1, 3)),
                'search_results_count': random.randint(0, 50),
                'filters_applied': random.choice([[], ['price'], ['category'], ['price', 'category']])
            }
        elif event_type == 'login':
            base_data = {
                'login_method': random.choice(['password', 'social_google', 'social_facebook']),
                'login_successful': random.random() > 0.02  # 98% success rate
            }
        
        return base_data

    def generate_batch_events(self, batch_size=50):
        """Generate a batch of events with realistic user behavior patterns"""
        events = []
        
        # Simulate some users being more active than others
        active_users = random.sample(range(1, 1001), random.randint(5, 20))
        
        for _ in range(batch_size):
            # 70% chance to use active user, 30% random user
            if random.random() < 0.7 and active_users:
                user_id = random.choice(active_users)
            else:
                user_id = random.randint(1, 1000)
                
            event = self.generate_user_event(user_id)
            events.append(event)
            
        return events

    def send_events(self):
        """Send events to Kafka with realistic streaming patterns"""
        batch_count = 0
        
        while True:
            try:
                # Simulate traffic patterns (more during day, less at night)
                current_hour = datetime.utcnow().hour
                if 8 <= current_hour <= 22:  # Daytime
                    batch_size = random.randint(30, 100)
                    delay = random.uniform(0.1, 0.5)
                else:  # Nighttime
                    batch_size = random.randint(5, 20)
                    delay = random.uniform(1, 3)
                
                events = self.generate_batch_events(batch_size)
                
                for event in events:
                    self.producer.send('user-events', value=event)
                
                batch_count += 1
                
                if batch_count % 10 == 0:
                    logger.info(f"Sent {batch_count} batches (~{batch_count * 50} events)")
                    self.producer.flush()
                
                # Simulate occasional traffic spikes
                if random.random() < 0.05:  # 5% chance of traffic spike
                    spike_events = self.generate_batch_events(200)
                    for event in spike_events:
                        self.producer.send('user-events', value=event)
                    logger.info("🚀 Traffic spike simulated - sent 200 additional events")
                
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error sending events: {e}")
                time.sleep(5)  # Wait before retry

if __name__ == "__main__":
    producer = EnhancedUserEventProducer()
    logger.info("Starting Enhanced User Event Producer with realistic data patterns")
    producer.send_events()