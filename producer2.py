from kafka import KafkaProducer
import json

bootstrap_servers = 'localhost:9092'
topic = 'test'

def filter_delivery_messages(data):
    return [msg for msg in data if msg['type'] == 'delivery']

with open("C:\\Users\\Sobha\\Documents\\BigData\\Last_Lab\\data.json", "r") as f:
    data = json.load(f)

delivery_messages = filter_delivery_messages(data)

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in delivery_messages:
    producer.send(topic, value=message)

producer.close()