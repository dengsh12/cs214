# consumer.py
import time
from confluent_kafka import Consumer, KafkaException
import utility

@utility.timer
def consume_messages(consumer_conf, topic, num_messages, log_interval):
    """
    消费者接收消息并计算延迟
    :param consumer_conf: dict, Consumer 配置信息
    :param topic: str, 消费的 Kafka Topic
    :param num_messages: int, 消费总消息数
    :param log_interval: int, 日志间隔
    :param latencies: list, 用于存放收到的消息延迟
    """
    print("🔴 consume_messages called")
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    latencies = []

    count = 0
    while count < num_messages:
        if count % log_interval == 0:
            print(f"🔴 消费者接收消息: {count}/{num_messages}")
        msg = consumer.poll(timeout=5.0)
        
        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())
        
        if count % log_interval == 0:
            print(f"内容是{msg.value().decode('utf-8')}")
        
        sent_time = float(msg.value().decode('utf-8'))
        latency = time.time() - sent_time
        latencies.append(latency)

        count += 1

    consumer.close()
    print("✅ 消费者完成接收消息")
    return latencies
