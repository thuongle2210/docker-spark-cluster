import csv
import random

import time,json,random
from datetime import datetime
from kafka import KafkaProducer

list_host = [
                '37.137.60.192', '37.137.60.193', '37.137.60.194', '37.137.60.195', '37.137.60.196',
                '37.137.60.197', '37.137.60.198', '37.137.60.199', '37.137.60.200', '37.137.60.201',
                '37.137.60.202', '37.137.60.203', '37.137.60.204', '37.137.60.205', '37.137.60.206',
                '37.137.60.207', '37.137.60.208', '37.137.60.209', '37.137.60.210', '37.137.60.211',
                '37.137.60.212', '37.137.60.213', '37.137.60.214', '37.137.60.215', '37.137.60.216'           
            ]
list_method = ['GET']
list_http_version = ['HTTP/1.0', 'HTTP/1.1', 'HTTP/2.0']
list_status = ['200', '201', '202', '301', '302', '304', '400', '401', '402', '403', '404', '405']
list_referer = [
            'https://www.google.com.vn', 'https://www.facebook.com.vn', 'https://www.youtube.com.vn',
            'https://thuongle.com.vn/root', 'https://tiktok.com', 'https://thuong1.com.vn',
            'https://thuong1.com.vn', 'https://thuong2.com.vn', 'https://thuong3.com.vn',
            'https://thuong6.com.vn', 'https://thuong5.com.vn', 'https://thuong4.com.vn',
            'https://thuong7.com.vn', 'https://thuong8.com.vn', 'https://thuong9.com.vn' 
        ]
list_agents = [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
            "Mozilla/5.0 (Windows NT 5.1; rv:8.0) Gecko/20100101 Firefox/8.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 OPR/57.0.3098.116 (Edition Campaign 34)",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0",
            "Mozilla/5.0 (Linux; Android 6.0.1; SAMSUNG SM-J700H Build/MMB29K) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/8.2 Chrome/63.0.3239.111 Mobile Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:52.59.12) Gecko/20160044 Firefox/52.59.12"
        ] 
list_product = [
                ('a101', 30), ('a102', 79), ('a103', 85), ('a104',66), ('a105',150),
                ('b101',559), ('b102',271), ('b103',97), ('b104',100), ('b105',115),
                ('c101',1048), ('c102',555), ('c103',1234), ('c104',123), ('c105',123),
            ]

def serializer(message):
    return message.encode("utf-8")
    # return json.dumps(message).encode("utf-8")
    
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=serializer
)

with open('/opt/kafka_code/listtimestamp.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    for row in csv_reader:
        host = random.choice(list_host)
        timestamp = row[0]
        method = random.choice(list_method)
        product, content_size = random.choice(list_product)
        endpoint = "products/"+product
        http_version = random.choice(list_http_version)
        status=random.choice(list_status)
        referer = random.choice(list_referer)
        agent = random.choice(list_agents)
        access_log_line = r'''%s - - [%s] "%s %s %s" %s %s "%s" "%s"'''%(host, timestamp, method, endpoint, http_version, status, content_size, referer, agent)
        print(access_log_line)
        producer.send("demo", access_log_line)