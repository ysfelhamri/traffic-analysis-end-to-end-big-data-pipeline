from kafka import KafkaConsumer
import json
from datetime import datetime as dt
from hdfs import InsecureClient

def store_data():       
    client = InsecureClient('http://localhost:9870')
    consumer = KafkaConsumer(
        'traffic-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  
        enable_auto_commit=True,
        group_id='location-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
    )
    #print(client.content("/data"))
    for message in consumer:
        event = message.value
        event_day = dt.fromtimestamp(int(event['event_time']//1000)).strftime('%Y-%m-%d')
        zone = event['zone']
        file_path = '/data/raw/traffic/'+event_day+'_'+zone+'.json'
        if client.content(file_path,strict=False):
            client.write(file_path, data=json.dumps(event), encoding='utf-8', append=True)
        else: 
            client.write(file_path, data=json.dumps(event), encoding='utf-8', append=False)

        
store_data()