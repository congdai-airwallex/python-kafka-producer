import random
import time
from kafka import KafkaProducer
import json
import threading

# To send messages synchronously
producer = KafkaProducer(bootstrap_servers='localhost:9092')


def gen_pre_event(index):
    event = {
        "clientId": "c",
        "paymentId": "q{}".format(index),
        "newBeneficiaryId": "",
        "payerAmountUsd": 0.00,
        "filtered": 80000+index,
        "createAt": 0
    }
    return str.encode(json.dumps(event))


def gen_post_event():
    event = {
        "clientId": "c",
        "paymentId": "p0",
        "newBeneficiaryId": "",
        "payerAmountUsd": 1.00,
        "filtered": 0,
        "createAt": 0
    }
    return str.encode(json.dumps(event))


def gen_pa_union_event():
    event = {
        "paymentAttemptId": "a1",
        "partitionKey": "k1",
        "aggregationKeyMappingValue": 10,
        "createAt": 0
    }
    return str.encode(json.dumps(event))


def send_pre_events():
    for i in range(2000):
        time.sleep(random.randint(0, 100)/1000)
        producer.send(topic='test', key=b'', value=gen_pre_event(i))


def send_post_events():
    for i in range(2000):
        time.sleep(random.randint(0, 100) / 1000)
        producer.send(topic='test2', key=b'', value=gen_post_event())


def send_pa_union_events():
    for i in range(20000):
        time.sleep(random.randint(0, 100) / 1000)
        producer.send(topic='test', key=b'', value=gen_pa_union_event())
        producer.flush()


t_list = []

# t1 = threading.Thread(target=send_pre_events, args=())
# t2 = threading.Thread(target=send_post_events, args=())
t3 = threading.Thread(target=send_pa_union_events, args=())
# t_list.append(t1)
# t_list.append(t2)
t_list.append(t3)
for t in t_list:
    t.start()

for t in t_list:
    t.join()
