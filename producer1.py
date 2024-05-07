from kafka import KafkaProducer
import json

bootstrap_servers = 'localhost:9092'
topic = 'test'

def filter_inventory_messages(data):
    return [msg for msg in data if msg['type'] == 'inventory']

with open("C:\\Users\\Sobha\\Documents\\BigData\\Last_Lab\\data.json", "r") as f:
    data = json.load(f)

inventory_messages = filter_inventory_messages(data)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in inventory_messages:
    producer.send(topic, value=message)

producer.close()