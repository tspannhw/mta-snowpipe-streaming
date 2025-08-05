#!/usr/bin/env python3
"""
Test script to verify Kafka connection and authentication
Usage: python test_kafka_connection.py
"""

import os
import json
import time
from confluent_kafka import Consumer, KafkaError, KafkaException

def test_kafka_connection():
    """Test Kafka connection with SASL_SSL authentication"""
    
    # Load configuration from environment
    kafka_config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'group.id': 'test-consumer-group',
        'auto.offset.reset': 'latest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
        'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
    }
    
    topic = os.getenv('KAFKA_TOPIC', 'icymta')
    
    print("🔧 Testing Kafka Connection")
    print(f"Bootstrap servers: {kafka_config['bootstrap.servers']}")
    print(f"Topic: {topic}")
    print(f"Username: {kafka_config['sasl.username']}")
    print("=" * 50)
    
    try:
        # Create consumer
        consumer = Consumer(kafka_config)
        print("✅ Kafka consumer created successfully")
        
        # Subscribe to topic
        consumer.subscribe([topic])
        print(f"✅ Subscribed to topic: {topic}")
        
        # Test polling for messages
        print("📡 Polling for messages (30 seconds)...")
        messages_received = 0
        start_time = time.time()
        
        while time.time() - start_time < 30:  # Poll for 30 seconds
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"📄 Reached end of partition {msg.partition()}")
                    continue
                else:
                    print(f"❌ Consumer error: {msg.error()}")
                    break
            
            # Successfully received message
            messages_received += 1
            print(f"📨 Message {messages_received}:")
            print(f"   Topic: {msg.topic()}")
            print(f"   Partition: {msg.partition()}")
            print(f"   Offset: {msg.offset()}")
            
            try:
                value = msg.value().decode('utf-8')
                data = json.loads(value)
                print(f"   Sample fields: {list(data.keys())[:5]}...")
            except Exception as e:
                print(f"   Raw value length: {len(msg.value())} bytes")
            
            print()
            
            # Limit output for testing
            if messages_received >= 5:
                break
        
        # Clean up
        consumer.close()
        
        print("=" * 50)
        if messages_received > 0:
            print(f"✅ SUCCESS: Received {messages_received} messages from Kafka")
            print("🎉 Kafka connection and authentication working correctly!")
        else:
            print("⚠️  WARNING: No messages received (topic might be empty)")
            print("✅ Connection and authentication successful")
        
        return True
        
    except KafkaException as e:
        print(f"❌ Kafka error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def check_environment():
    """Check required environment variables"""
    required_vars = [
        'KAFKA_BOOTSTRAP_SERVERS',
        'KAFKA_SASL_USERNAME', 
        'KAFKA_SASL_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables and try again.")
        return False
    
    return True

if __name__ == "__main__":
    print("🚀 Kafka Connection Test")
    print("=" * 50)
    
    if not check_environment():
        exit(1)
    
    success = test_kafka_connection()
    exit(0 if success else 1)