import time,json,random
from datetime import datetime
from kafka import KafkaProducer

def serializer(message):
    return message.encode("utf-8")
    
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=serializer
)

if __name__=="__main__":
    with open("/opt/kafka_code/access_log.log", 'r') as log:
        lines = log.readlines()
        for message in lines:
            producer.send("demo",message[:-1])
            time.sleep(0.1)