import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

TOPIC_NAME = 'ecommerce_order_events'
BOOTSTRAP_SERVERS = ['localhost:9092', 'kafka:9092']

EVENT_TYPES = ['order_created', 'payment_completed', 'order_shipped', 'order_delivered', 'order_cancelled', 'order_refunded']

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )

def generate_order_event():
    order_id = f'ord_{random.randint(100000, 999999)}'
    user_id = f'user_{random.randint(1000, 9999)}'
    item_id = f'item_{random.randint(100, 500)}'
    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10.0, 500.0), 2)
    
    return {
        'order_id': order_id,
        'user_id': user_id,
        'item_id': item_id,
        'event_type': event_type,
        'order_amount': amount,
        'event_timestamp': datetime.utcnow().isoformat(),
        'payment_method': random.choice(['CreditCard', 'KakaoPay', 'NaverPay', 'AccountTransfer']),
        'region': random.choice(['Seoul', 'Gyeonggi', 'Busan', 'Incheon', 'Daegu'])
    }

if __name__ == '__main__':
    producer = get_producer()
    print(f'Streaming order events to {TOPIC_NAME}...')
    try:
        while True:
            evt = generate_order_event()
            producer.send(TOPIC_NAME, value=evt)
            time.sleep(random.uniform(0.05, 0.2))
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
