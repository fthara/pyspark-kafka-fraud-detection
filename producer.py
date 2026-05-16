import time
import json
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'client.id': 'python-producer'
}

producer = Producer(conf)

TOPIC_NAME = "transactions"

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Fail to send message to kafka: {err}")
    else:
        print(f"✅ Success to send message to kafka, Topic: {msg.topic()}, Partition: [{msg.partition()}]")

print("Producer Started! Sending transactions to kafka... (Press Ctrl+C to exit)")

try:
    while True:
        # Generating a ordinary transaction
        user_id = f"user_{random.randint(100, 150)}"

        tx = {
            "transaction_id": fake.uuid4(),
            "user_id": user_id,
            "amount": round(random.uniform(10.0, 500.0), 2),
            "card_brand": random.choice(["Visa", "Mastercard", "American Express"]),
            "country": "BR",
            "merchant": fake.company(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        producer.produce(TOPIC_NAME, value=json.dumps(tx).encode('utf-8'), callback=delivery_report)
        print(f"Ordinary: {user_id} buy in {tx['country']}")

        # 15% chance of injecting an immediate fraud.
        if random.random() < 0.15:
            print(f"Simulation: Injecting a fraudulent transaction to user {user_id}")

            tx_fraud = {
                "transaction_id": fake.uuid4(),
                "user_id": user_id,
                "amount": round(random.uniform(800.0, 3000.0), 2),
                "card_brand": tx["card_brand"],
                "country": "US",
                "merchant": "International Store",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            producer.produce(TOPIC_NAME, value=json.dumps(tx_fraud).encode('utf-8'), callback=delivery_report)
            print(f"Fraud: {user_id} buy in US")

        producer.poll(0)
        producer.flush()
        time.sleep(random.uniform(0.5, 2.0))
except KeyboardInterrupt:
    print("Producer stopped!")
finally:
    producer.close()
