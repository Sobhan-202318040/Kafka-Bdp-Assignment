import json

with open("C:\\Users\\Sobha\\Documents\\BigData\\Last_Lab\\data.json", "r") as f:
    data = json.load(f)

def filter_inventory_messages(data):
    return [msg for msg in data if msg['type'] == 'inventory']

def filter_delivery_messages(data):
    return [msg for msg in data if msg['type'] == 'delivery']

inventory_messages = filter_inventory_messages(data)
print("Inventory messages:", inventory_messages)

delivery_messages = filter_delivery_messages(data)
print("Delivery messages:", delivery_messages)