import time,json,random
from datetime import datetime
from kafka import KafkaProducer

def serializer(message):
    return message.encode("utf-8")
    # return json.dumps(message).encode("utf-8")
    
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=serializer
)

if __name__=="__main__":
    for i in range(10000000):
        dummy_messages= r'''37.137.60.194 - - [22/Jan/2019:04:42:20 +0330] "GET products/b105 HTTP/1.1" 405 115 "https://thuongle.com.vn/root" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"'''
        print(f"Producing message {datetime.now()} | Message = {str(dummy_messages)}")
        producer.send("demo",dummy_messages)
        time.sleep(2)