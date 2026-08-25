import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

TOPIC_NAME = 'clinical_search_logs'
BOOTSTRAP_SERVERS = ['localhost:9092', 'kafka:9092']

USER_AGENTS = ['Mozilla/5.0 (Macintosh; Intel Mac OS X)', 'Mozilla/5.0 (Windows NT 10.0)', 'Mozilla/5.0 (iPhone)']
SEARCH_KEYWORDS = ['diabetes symptoms', 'hypertension guidelines', 'aspirin dosage', 'covid19 treatment', 'mri lumbar spine', 'oncology clinical trial']
ACTIONS = ['search', 'click_document', 'download_pdf', 'filter_category', 'bookmark']
CATEGORIES = ['Clinical Guidelines', 'Pharmacology', 'Radiology', 'Pathology', 'General Medicine']

def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )

def generate_log():
    user_id = f'user_{random.randint(1000, 9999)}'
    action = random.choice(ACTIONS)
    keyword = random.choice(SEARCH_KEYWORDS) if action in ['search', 'click_document'] else None
    
    return {
        'event_id': f'evt_{int(time.time()*1000)}_{random.randint(100, 999)}',
        'timestamp': datetime.utcnow().isoformat(),
        'user_id': user_id,
        'action': action,
        'search_keyword': keyword,
        'document_id': f'doc_{random.randint(100, 500)}' if action in ['click_document', 'download_pdf', 'bookmark'] else None,
        'category': random.choice(CATEGORIES),
        'duration_sec': round(random.uniform(0.5, 120.0), 2),
        'ip_address': f'192.168.{random.randint(1, 254)}.{random.randint(1, 254)}',
        'user_agent': random.choice(USER_AGENTS)
    }

if __name__ == '__main__':
    producer = get_producer()
    print(f'Starting real-time log producer to topic: {TOPIC_NAME}')
    try:
        count = 0
        while True:
            log_data = generate_log()
            producer.send(TOPIC_NAME, value=log_data)
            count += 1
            if count % 100 == 0:
                print(f'Sent {count} events...')
            time.sleep(random.uniform(0.01, 0.1))
    except KeyboardInterrupt:
        print('Stopping producer...')
    finally:
        producer.flush()
        producer.close()
