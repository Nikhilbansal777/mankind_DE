"""
Kafka Consumer - AI Recommendation Engine
Consumes user events from Kafka and generates real-time recommendations
"""

import json
import time
import os
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, List, Any

# Import from your existing code
from AiRecommendation_1 import (
    UserProfile, create_simulated_users, load_product_catalog, 
    create_dummy_product_catalog, AIEngine, MetricsTracker
)

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'group_id': 'ai-recommendation-group',
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
}
 
TOPICS = {
    'user_events': 'user-events',
    'recommendations': 'recommendations',
    'metrics': 'metrics'
}

class AIRecommendationConsumer:
    """
    Kafka consumer that processes user events and generates AI-powered recommendations
    """ 
    
    def __init__(self, product_catalog: List[Dict[str, Any]]):
        """
        Initialize the AI recommendation consumer
        
        Args:
            product_catalog: List of product dictionaries
        """
        self.products = product_catalog
        self.ai_engine = AIEngine(product_catalog)
        self.metrics_tracker = MetricsTracker()
        
        # Track user sessions
        self.user_sessions = {}
        self.processed_events = 0
        self.start_time = time.time()
        
        # Setup Kafka consumer
        self.consumer = KafkaConsumer(
            TOPICS['user_events'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            auto_offset_reset=KAFKA_CONFIG['auto_offset_reset'],
            enable_auto_commit=KAFKA_CONFIG['enable_auto_commit'],
            group_id=KAFKA_CONFIG['group_id'],
            value_deserializer=KAFKA_CONFIG['value_deserializer']
        )
        
        # Setup Kafka producer for sending recommendations
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=KAFKA_CONFIG['value_serializer']
        )
        
        print(f"AI Consumer initialized with {len(self.products)} products")
        print(f"Connected to Kafka broker: {KAFKA_CONFIG['bootstrap_servers'][0]}")
        print(f"Consumer group: {KAFKA_CONFIG['group_id']}")
    
    def create_user_profile_from_data(self, user_profile_data: Dict[str, Any], user_id: int) -> UserProfile:
        """
        Create a UserProfile object from event data
        
        Args:
            user_profile_data: User profile dictionary from event
            user_id: User ID
            
        Returns:
            UserProfile object
        """
        profile = UserProfile(
            user_id=user_id,
            preferred_categories=user_profile_data.get('preferred_categories', []),
            preferred_features=user_profile_data.get('preferred_features', []),
            price_sensitivity=user_profile_data.get('price_sensitivity', 0.5)
        )
        
        # Restore viewing and purchase history
        for product_id in user_profile_data.get('viewing_history', []):
            profile.view_product(product_id)
        
        for product_id in user_profile_data.get('purchase_history', []):
            profile.purchase_product(product_id)
            
        return profile
    
    def process_view_event(self, event_data: Dict[str, Any]):
        """
        Process a product view event and generate recommendations
        
        Args:
            event_data: Event data dictionary
        """
        viewed_product = event_data['product']
        user_id = event_data['user_id']
        user_profile_data = event_data.get('user_profile')
        
        print(f"\n🔍 Processing View Event:")
        print(f"   User: {user_id}")
        print(f"   Product: {viewed_product['name']}")
        print(f"   Category: {viewed_product.get('category', 'Unknown')}")
        print(f"   Price: ${viewed_product.get('price', 0)}")
        
        # Create or update user profile
        user_profile = None
        if user_profile_data and user_id:
            user_profile = self.create_user_profile_from_data(user_profile_data, user_id)
            self.user_sessions[user_id] = user_profile
        
        # Generate recommendations using AI engine
        start_time = time.time()
        recommendations = self.ai_engine.get_recommendations(
            viewed_product, 
            user_profile=user_profile,
            count=5
        )
        recommendation_time = time.time() - start_time
        
        # Record metrics if user profile exists
        if user_profile:
            self.metrics_tracker.record_recommendations(
                user_profile, recommendations, self.products
            )
        
        # Create recommendation response
        recommendation_data = {
            'event_type': 'recommendations_generated',
            'timestamp': time.time(),
            'user_id': user_id,
            'viewed_product_id': viewed_product['id'],
            'viewed_product_name': viewed_product['name'],
            'recommendations': recommendations,
            'recommendation_count': len(recommendations),
            'processing_time_ms': recommendation_time * 1000,
            'has_user_profile': user_profile is not None
        }
        
        # Send recommendations to Kafka topic
        try:
            self.producer.send(TOPICS['recommendations'], recommendation_data)
            print(f"   ✅ Generated {len(recommendations)} recommendations in {recommendation_time*1000:.2f}ms")
            
            # Display recommendations
            for i, rec in enumerate(recommendations, 1):
                print(f"      {i}. {rec['name']} (${rec.get('price', 0)}) - {rec.get('category', 'Unknown')}")
                
        except Exception as e:
            print(f"   ❌ Error sending recommendations: {e}")
        
        # Send metrics periodically (every 10 events)
        if self.processed_events % 10 == 0:
            self.send_metrics_update()
    
    def process_purchase_event(self, event_data: Dict[str, Any]):
        """
        Process a product purchase event
        
        Args:
            event_data: Event data dictionary
        """
        purchased_product = event_data['product']
        user_id = event_data['user_id']
        
        print(f"\n💰 Purchase Event:")
        print(f"   User {user_id} purchased: {purchased_product['name']}")
        print(f"   Price: ${purchased_product.get('price', 0)}")
        
        # Update user session if exists
        if user_id in self.user_sessions:
            self.user_sessions[user_id].purchase_product(purchased_product['id'])
        
        # Send purchase confirmation event
        purchase_confirmation = {
            'event_type': 'purchase_confirmed',
            'timestamp': time.time(),
            'user_id': user_id,
            'product_id': purchased_product['id'],
            'product_name': purchased_product['name'],
            'price': purchased_product.get('price', 0)
        }
        
        try:
            self.producer.send(TOPICS['recommendations'], purchase_confirmation)
            print(f"   ✅ Purchase confirmation sent")
        except Exception as e:
            print(f"   ❌ Error sending purchase confirmation: {e}")
    
    def send_metrics_update(self):
        """Send current metrics to Kafka"""
        current_metrics = self.metrics_tracker.get_summary()
        
        # Add runtime statistics
        runtime = time.time() - self.start_time
        current_metrics.update({
            'runtime_seconds': runtime,
            'events_processed': self.processed_events,
            'events_per_second': self.processed_events / runtime if runtime > 0 else 0,
            'active_user_sessions': len(self.user_sessions)
        })
        
        metrics_data = {
            'event_type': 'metrics_update',
            'timestamp': time.time(),
            'metrics': current_metrics
        }
        
        try:
            self.producer.send(TOPICS['metrics'], metrics_data)
            print(f"   📊 Metrics Update - Precision: {current_metrics['precision']:.4f}, "
                  f"Recall: {current_metrics['recall']:.4f}, "
                  f"F1: {current_metrics['f1']:.4f}")
        except Exception as e:
            print(f"   ❌ Error sending metrics: {e}")
    
    def display_status(self):
        """Display current consumer status"""
        runtime = time.time() - self.start_time
        events_per_second = self.processed_events / runtime if runtime > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"AI Consumer Status")
        print(f"{'='*50}")
        print(f"Runtime: {runtime:.2f} seconds")
        print(f"Events Processed: {self.processed_events}")
        print(f"Events/Second: {events_per_second:.2f}")
        print(f"Active User Sessions: {len(self.user_sessions)}")
        
        # Display current metrics
        metrics = self.metrics_tracker.get_summary()
        print(f"Current Metrics:")
        print(f"  Precision: {metrics['precision']:.4f}")
        print(f"  Recall: {metrics['recall']:.4f}")
        print(f"  F1 Score: {metrics['f1']:.4f}")
        print(f"{'='*50}\n")
    
    def start_consuming(self):
        """Start consuming events from Kafka"""
        print(f"\n🚀 AI Recommendation Consumer Started!")
        print(f"Listening on topic: {TOPICS['user_events']}")
        print(f"Waiting for events...\n")
        
        try:
            for message in self.consumer:
                event_data = message.value
                event_type = event_data.get('event_type', 'unknown')
                
                # Process different event types
                if event_type == 'product_view':
                    self.process_view_event(event_data)
                elif event_type == 'product_purchase':
                    self.process_purchase_event(event_data)
                else:
                    print(f"⚠️ Unknown event type: {event_type}")
                
                self.processed_events += 1
                
                # Display status every 25 events
                if self.processed_events % 25 == 0:
                    self.display_status()
                
        except KeyboardInterrupt:
            print("\n⚠️ Consumer interrupted by user")
        except Exception as e:
            print(f"\n❌ Consumer error: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown the consumer"""
        print("\n🛑 Shutting down AI Consumer...")
        
        # Send final metrics
        self.send_metrics_update()
        
        # Close connections
        self.consumer.close()
        self.producer.flush()
        self.producer.close()
        
        # Display final summary
        runtime = time.time() - self.start_time
        print(f"\n{'='*50}")
        print(f"Final Summary")
        print(f"{'='*50}")
        print(f"Total Runtime: {runtime:.2f} seconds")
        print(f"Total Events Processed: {self.processed_events}")
        print(f"Average Events/Second: {self.processed_events/runtime:.2f}")
        print(f"User Sessions Created: {len(self.user_sessions)}")
        
        final_metrics = self.metrics_tracker.get_summary()
        print(f"Final Metrics:")
        print(f"  Precision: {final_metrics['precision']:.4f}")
        print(f"  Recall: {final_metrics['recall']:.4f}")
        print(f"  F1 Score: {final_metrics['f1']:.4f}")
        print(f"  Total Recommendations: {final_metrics['total_recommendations']}")
        print(f"  Relevant Recommendations: {final_metrics['relevant_recommendations']}")
        print(f"{'='*50}")
        print("Consumer shutdown complete! 👋")


def main():
    """
    Main function to run the AI recommendation consumer
    """
    # Setup paths
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    product_catalog_path = os.path.join(data_dir, "dummy_products.json")
    
    # Create dummy product catalog if it doesn't exist
    if not os.path.exists(product_catalog_path):
        print("Creating dummy product catalog...")
        create_dummy_product_catalog(product_catalog_path)
    
    # Load product catalog
    products = load_product_catalog(product_catalog_path)
    
    print("AI Recommendation Consumer Setup:")
    print(f"Products loaded: {len(products)}")
    print(f"Kafka broker: {KAFKA_CONFIG['bootstrap_servers'][0]}")
    print(f"Consumer group: {KAFKA_CONFIG['group_id']}")
    
    # Create and start consumer
    consumer = AIRecommendationConsumer(products)
    consumer.start_consuming()


if __name__ == "__main__":
    main()