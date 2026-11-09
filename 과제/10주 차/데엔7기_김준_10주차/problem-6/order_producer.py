import argparse
import csv
import json
import os
import sys
import time
from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser(description="Order Events Kafka Producer")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        default="order-events",
        help="Target Kafka topic (default: order-events)"
    )
    parser.add_argument(
        "--csv-path",
        default=os.path.join(os.path.dirname(__file__), "..", "data", "order_events.csv"),
        help="Path to order_events.csv"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.001,
        help="Delay between records in seconds (default: 0.001)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    csv_path = os.path.abspath(args.csv_path)
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Connecting to Kafka at {args.bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3
    )

    print(f"Reading orders from {csv_path} and sending to topic '{args.topic}'...")
    start_time = time.time()
    sent_count = 0

    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # order_id,product_id,order_time,quantity
            # ISO-8601 format (YYYY-MM-DDTHH:MM:SS) for TimeBasedPartitioner RecordField extraction
            raw_time = row["order_time"].strip()
            iso_time = raw_time.replace(" ", "T")

            record = {
                "order_id": row["order_id"].strip(),
                "product_id": row["product_id"].strip(),
                "order_time": iso_time,
                "quantity": int(row["quantity"].strip())
            }

            producer.send(
                topic=args.topic,
                key=record["order_id"],
                value=record
            )
            sent_count += 1

            if sent_count % 1000 == 0:
                print(f"Sent {sent_count} records...")

            if args.delay > 0:
                time.sleep(args.delay)

    producer.flush()
    elapsed = time.time() - start_time
    print(f"Successfully sent all {sent_count} records to '{args.topic}' in {elapsed:.2f} seconds.")
    producer.close()


if __name__ == "__main__":
    main()
