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
                key=event['user_id'],
                value=json.dumps(event),
                callback=self.delivery_callback
            )
            self.producer.flush()
            
        except Exception as e:
            print(f"❌ Error sending to Kafka: {e}")
    
    def write_to_file(self, event):
        """Write event to file for Flink filesystem connector"""
        
        # Create timestamped filename
        timestamp = int(time.time())
        filename = f"events_{timestamp}.json"
        filepath = os.path.join(self.data_dir, filename)
        
        # Write single event per file (Flink reads better this way)
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
                # Generate event
                event = self.generate_event()
                
                # Display event
                print(f"\n📨 Event {self.event_counter}:")
                print(f"   👤 User: {event['user_id']}")
                print(f"   🎯 Action: {event['event_type']}")
                print(f"   💰 Amount: ${event['amount']}")
                print(f"   📄 Page: {event['page_url']}")
                
                if dual_output:
                    # Send to both Kafka and file
                    self.send_to_kafka(event)
                    self.write_to_file(event)
                else:
                    # Send only to file (for Flink filesystem connector)
                    self.write_to_file(event)
                
                # Wait before next event
                time.sleep(2)
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping live data generation")
            print(f"📊 Generated {self.event_counter} total events")
    
    def generate_batch(self, count=10):
        """Generate a batch of events"""
        
        print(f"📦 Generating batch of {count} events...")
        
        # Create batch file
        timestamp = int(time.time())
        filename = f"batch_{timestamp}.json"
        filepath = os.path.join(self.data_dir, filename)
        
        with open(filepath, 'w') as f:
            for i in range(count):
                event = self.generate_event()
                f.write(json.dumps(event) + '\n')
                
                print(f"📨 Generated: {event['event_type']} by {event['user_id']} ${event['amount']}")
                time.sleep(0.1)  # Small delay for realistic timestamps
        
        print(f"✅ Batch written to: {filename}")
        return filepath

def main():
    """Main function"""
    
    print("🎯 LIVE DATA PRODUCER")
    print("=" * 30)
    
    producer = LiveDataProducer()
    
    print("\nChoose mode:")
    print("1. 🔄 Live streaming (continuous)")
    print("2. 📦 Generate batch file")
    print("3. 🎪 Demo mode (file only)")
    
    try:
        choice = input("\nEnter choice (1-3): ").strip()
        
        if choice == '1':
            producer.start_live_generation(dual_output=True)
            
        elif choice == '2':
            count = input("How many events? (default 20): ").strip()
            count = int(count) if count.isdigit() else 20
            producer.generate_batch(count)
            
        elif choice == '3':
            print("🎪 Demo mode - file output only")
            producer.start_live_generation(dual_output=False)
            
        else:
            print("❌ Invalid choice")
            
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")

if __name__ == "__main__":
    main()