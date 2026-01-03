from kafka import KafkaConsumer
import json
from datetime import datetime as dt
from hdfs import InsecureClient
import time

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
    for message in consumer:
        event = message.value
        # Getting a correct timestamp value by dividing by 1000
        event_timestamp = dt.fromtimestamp(int(event['event_time']//1000))
        event_day = event_timestamp.strftime('%Y_%m_%d')
        zone = event['zone']
        # Grouping JSON files by zone then by day 
        file_path = '/data/raw/traffic/'+zone+'/'+event_day+'/'+str(time.time())+'_'+zone+'.json'
        # Sanitizing the file path before sending to HDFS
        file_path = "".join(c for c in file_path if c.isalpha() or c.isdigit() or c in (' ','_','.','/')).rstrip()
        client.write(file_path, data=json.dumps(event), encoding='utf-8')

        
store_data()