from kafka import KafkaConsumer
import json

bootstrap_servers = 'localhost:9092'
topic = 'test'

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

for message in consumer:
    if message.value['type'] == 'delivery':
        print("Delivery message received:", message.value)

consumer.close()