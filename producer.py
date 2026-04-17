import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

users = [f"U{100+i}" for i in range(10)]
merchants = ["Amazon", "Walmart", "Target", "BestBuy", "Nike"]
cities = ["Dallas", "Chicago", "New York", "Houston", "San Francisco"]
countries = ["USA", "USA", "USA", "Canada", "India"]
device_types = ["mobile", "web", "tablet"]
payment_methods = ["credit_card", "debit_card", "wallet", "upi"]

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "timestamp": datetime.now().isoformat(),
        "amount": round(random.uniform(10, 2000), 2),
        "merchant": random.choice(merchants),
        "city": random.choice(cities),
        "country": random.choice(countries),
        "device_type": random.choice(device_types),
        "payment_method": random.choice(payment_methods)
    }

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic_name = "transactions"

print("Sending transactions to Kafka...")

while True:
    transaction = generate_transaction()
    producer.send(topic_name, value=transaction)
    print("Sent:", transaction)
    time.sleep(1)