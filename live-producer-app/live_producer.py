# This is your provided script, adapted to work inside Docker by
# reading the Kafka server location from an environment variable.

import json
import time
import random
import os
from datetime import datetime
from confluent_kafka import Producer

class LiveDataProducer:
    """Producer that generates live streaming data"""
    
    def __init__(self):
        # Kafka configuration - UPDATED to read from environment variable
        kafka_server = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_config = {
            'bootstrap.servers': kafka_server,
            'client.id': 'live-producer'
        }
        
        # Create producer
        self.producer = Producer(self.kafka_config)
        
        # Create data directory for file output
        self.data_dir = "./flink-data"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Initialize counters
        self.event_counter = 0
        
        print(f"🚀 Live Data Producer initialized (Kafka Server: {kafka_server})")
        print(f"📁 Data directory: {self.data_dir}")
    
    def generate_event(self):
        """Generate realistic user event"""
        
        users = ['alice', 'bob', 'charlie', 'diana', 'eve', 'frank', 'grace']
        event_types = ['click', 'view', 'purchase', 'add_to_cart', 'login', 'logout']
        pages = ['/home', '/products', '/cart', '/checkout', '/profile', '/search']
        
        event_type = random.choice(event_types)
        amount = 0.0
        
        # Add realistic amounts for purchase events
        if event_type == 'purchase':
            amount = round(random.uniform(10.0, 500.0), 2)
        elif event_type == 'add_to_cart':
            amount = round(random.uniform(5.0, 200.0), 2)
        
        self.event_counter += 1
        
        event = {
            'event_id': self.event_counter,
            'user_id': random.choice(users),
            'event_type': event_type,
            'timestamp': int(time.time() * 1000),
            'page_url': random.choice(pages),
            'session_id': f'sess_{random.randint(1000, 9999)}',
            'amount': amount,
            'generated_at': datetime.now().isoformat()
        }
        
        return event
    
    def delivery_callback(self, err, msg):
        """Kafka delivery callback"""
        if err:
            print(f"❌ Delivery failed: {err}")
        else:
            print(f"✅ Event {self.event_counter} delivered to {msg.topic()}")
    
    def send_to_kafka(self, event):
        """Send event to Kafka topic"""
        try:
            self.producer.produce(
                topic='user-events',
                key=str(event['user_id']), # Key should be a string
                value=json.dumps(event),
                callback=self.delivery_callback
            )
            # Flush messages to send them immediately
            self.producer.poll(0)
            
        except Exception as e:
            print(f"❌ Error sending to Kafka: {e}")
    
    def write_to_file(self, event):
        """Write event to file for Flink filesystem connector"""
        
        # Create timestamped filename
        timestamp = int(time.time())
        filename = f"events_{timestamp}_{self.event_counter}.json"
        filepath = os.path.join(self.data_dir, filename)
        
        # Write single event per file
        with open(filepath, 'w') as f:
            f.write(json.dumps(event) + '\n')
        
        print(f"📄 Written to file: {filename}")
    
    def start_live_generation(self, dual_output=True):
        """Start generating live data"""
        
        print("🔄 Starting live data generation...")
        print("📊 Generating 1 event every 2 seconds")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                event = self.generate_event()
                
                print(f"\n📨 Event {self.event_counter}:")
                print(f"   👤 User: {event['user_id']}")
                print(f"   🎯 Action: {event['event_type']}")
                
                if dual_output:
                    self.send_to_kafka(event)
                    self.write_to_file(event)
                else:
                    self.write_to_file(event)
                
                self.producer.flush()
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping live data generation")
            print(f"📊 Generated {self.event_counter} total events")

def main():
    """Main function"""
    
    print("🎯 LIVE DATA PRODUCER")
    print("=" * 30)
    
    producer = LiveDataProducer()
    
    print("\nChoose mode:")
    print("1. 🔄 Live streaming (Kafka & File)")
    print("2. 🎪 Demo mode (File only)")
    
    try:
        choice = input("\nEnter choice (1-2): ").strip()
        
        if choice == '1':
            producer.start_live_generation(dual_output=True)
        elif choice == '2':
            print("🎪 Demo mode - file output only")
            producer.start_live_generation(dual_output=False)
        else:
            print("❌ Invalid choice")
            
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")

if __name__ == "__main__":
    main()
