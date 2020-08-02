# import time
# from pykafka import KafkaClient
#
# client = KafkaClient(hosts="127.0.0.1:9092")
# topic = client.topics[b'test']
#
#
# def consumer():
#     with topic.get_producer(delivery_reports=True) as producer:
#         print('init finished..')
#         next_data = ''
#         while True:
#             if next_data:
#                 producer.produce(str(next_data).encode())
#             next_data = yield True
#
#
# def feed():
#     c = consumer()
#     next(c)
#     for i in range(1000):
#         c.send(i)
#
# start = time.time()
# feed()
# end = time.time()
# print(f'直到把所有数据塞入Kafka，一共耗时：{end - start}秒')

# config.py
SERVER = '192.168.1.15:9093'
# USERNAME = 'kingname'
# PASSWORD = 'kingnameisgod'
TOPIC = 'first'

import json
import time
import datetime
from kafka import KafkaProducer


# producer = KafkaProducer(bootstrap_servers=SERVER,
#                          value_serializer=lambda m: json.dumps(m).encode())
#
# for i in range(100):
#     data = {'num': i, 'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#     producer.send(TOPIC, data)
#     time.sleep(1)
#


# import datetime
# import json
# import time
# from kafka import KafkaProducer
# producer = KafkaProducer(bootstrap_servers='192.168.1.15:9093')
# print(producer.config)
# future = producer.send('first', json.dumps(
#     {"method": "get", "step": "1", "type": "test", "testName": "kafka",
#      "cid": "{0}".format(datetime.datetime.now().strftime('%Y%m%d%H%M%S')),
#      "info": "demo{}".format(1)}).encode())
# record_metadata = future.get(timeout=10)
# print( record_metadata, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))



import json
import time
import datetime

from kafka import KafkaProducer

SERVER  = "10.0.3.44:9092"
TOPIC  = "zmj_ssecond"
producer = KafkaProducer(bootstrap_servers=SERVER,
                         value_serializer=lambda m: json.dumps(m).encode(),
                         acks=-1
                         # security_protocol="SASL_PLAINTEXT",
                         # sasl_mechanism="PLAIN",
                         # sasl_plain_username=config.USERNAME,
                         # sasl_plain_password=config.PASSWORD
                         )

for i in range(100):
    data = {'num': i, 'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    print(data)
    producer.send(TOPIC, data)
    time.sleep(1)








