import json
import random
import uuid
from datetime import datetime

users = [f"U{100+i}" for i in range(10)]
merchants = ["Amazon", "Walmart", "Target", "BestBuy", "Nike"]
cities = ["Dallas", "Chicago", "New York", "Houston", "San Francisco"]
countries = ["USA", "USA", "USA", "Canada", "India"]
device_types = ["mobile", "web", "tablet"]
payment_methods = ["credit_card", "debit_card", "wallet", "upi"]

def generate_transaction():
    transaction = {
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
    return transaction

if __name__ == "__main__":
    for _ in range(5):
        print(json.dumps(generate_transaction(), indent=2))