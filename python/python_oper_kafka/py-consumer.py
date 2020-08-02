from kafka import KafkaConsumer

from confluent_kafka import Consumer, KafkaError,TopicPartition

def helloworldConsumer():
    c = Consumer({
        'bootstrap.servers': '10.0.3.44:9092',
        'group.id': 'action-test',
        'auto.offset.reset': 'earliest', # 首次运行从头开始消费
        'enable.auto.commit': True,  # 自动提交位移
        "auto.commit.interval.ms": "1000",  # 间隔时间
    })
    topic = 'zmj_ssecond'

    c.subscribe([topic])
    cd = c.list_topics()
    print(cd.cluster_id)
    print(cd.controller_id)
    print(cd.brokers)
    print(cd.topics)
    print(cd.orig_broker_id)
    print(cd.orig_broker_name)
    while True:
        msg = c.poll(1.0)
        # print(msg)
        if msg is None:
            continue

        print('topic:{topic}, partition:{partition}, offset:{offset}, headers:{headers}, key:{key}, msg:{msg}, timestamp:{timestamp}'.format(topic=msg.topic(),
                                                                                                                                             msg=msg.value(),
                                                                                                                                             headers=msg.headers(),
                                                                                                                                             key=msg.key(),
                                                                                                                                             offset=msg.offset(),
                                                                                                                                             partition=msg.partition(),
                                                                                                                                             timestamp=msg.timestamp()))



# 手动提交位移
def commitedOffset():
    c = Consumer({
        'bootstrap.servers': '10.0.3.44:9092',
        'group.id': 'action-test',
        'auto.offset.reset': 'earliest', # 首次运行从头开始消费
        'enable.auto.commit': False,  # 自动提交位移
        "auto.commit.interval.ms": "1000",  # 间隔时间,如果为False 间隔时间将是无效的
    })
    topic = 'zmj_ssecond'

    c.subscribe([topic])
    cd = c.list_topics()
    print(cd.cluster_id)
    print(cd.controller_id)
    print(cd.brokers)
    print(cd.topics)
    print(cd.orig_broker_id)
    print(cd.orig_broker_name)

    while True:
        msg = c.poll(1.0)
        # print(msg)
        if msg is None:
            continue

        print('topic:{topic}, partition:{partition}, offset:{offset}, headers:{headers}, key:{key}, msg:{msg}, '
              'timestamp:{timestamp}'.format(topic=msg.topic(), msg=msg.value(),headers=msg.headers(),key=msg.key(),
                                             offset=msg.offset(),partition=msg.partition(),timestamp=msg.timestamp()))
        c.commit()


if __name__ == '__main__':
    # 基本操作
    # helloworldConsumer()
    commitedOffset()
