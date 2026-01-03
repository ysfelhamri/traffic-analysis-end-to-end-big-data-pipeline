from kafka import KafkaProducer
import json
from data_generation import generate_data
import time 

def send_data(num_events,delay):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    while True:
        data = json.loads(generate_data(num_events))
        for event in data:   
            producer.send('traffic-events', value= event)
        time.sleep(delay)

send_data(5,5)