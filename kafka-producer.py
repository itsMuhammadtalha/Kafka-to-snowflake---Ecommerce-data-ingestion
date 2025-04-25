from kafka import KafkaProducer
import json, time, random
from faker import Faker

fake = Faker()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

products = [
    {"product_id": "P123", "product_name": "Wireless Mouse", "category": "Accessories", "price": 2500},
    {"product_id": "P124", "product_name": "Keyboard", "category": "Accessories", "price": 3000},
    {"product_id": "P125", "product_name": "Monitor", "category": "Electronics", "price": 20000}
]

for i in range(100):
    product = random.choice(products)
    message = {
        "order_id": f"O{1000+i}",
        "order_timestamp": fake.iso8601(),
        "customer_id": f"C{random.randint(100, 999)}",
        "customer_name": fake.name(),
        "customer_region": fake.state(),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": random.randint(1, 5)
    }
    producer.send("orders", message)
    print(f"Sent: {message}")
    time.sleep(1)
