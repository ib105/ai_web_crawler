from confluent_kafka import Producer
import json
import os

def get_kafka_producer():
    """Initialize Kafka producer with connection settings"""
    config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_USERNAME'),
        'sasl.password': os.getenv('KAFKA_PASSWORD'),
    }
    return Producer(config)

def send_news_to_kafka(news_items: list, topic: str):
    """Send news items to Kafka topic"""
    producer = get_kafka_producer()
    
    for news in news_items:
        try:
            producer.produce(
                topic=topic,
                key=news['title'].encode('utf-8'),
                value=json.dumps(news).encode('utf-8')
            )
        except Exception as e:
            print(f"Error sending to Kafka: {e}")
    
    producer.flush()
    print(f"Sent {len(news_items)} news items to Kafka topic: {topic}")