from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,
    'acks': 'all',
    'retries': 5,
    'max.in.flight.requests.per.connection': 1
}

producer = Producer(conf)
print('Idempotent Kafka producer initialized.')
