# producer.py
import time
from confluent_kafka import Producer
import utility

@utility.timer
def produce_messages(producer_conf, topic, num_messages, log_interval):
    """
    生产者发送带有时间戳的消息
    :param producer_conf: dict, Producer 配置信息
    :param topic: str, 发送到的 Kafka Topic
    :param num_messages: int, 发送总消息数
    :param log_interval: int, 日志间隔
    """
    producer = Producer(producer_conf)

    for i in range(num_messages):
        if i % log_interval == 0:
            print(f"🚀 生产者发送消息: {i}/{num_messages}")
        timestamp = time.time()

        message_sent = False
        while not message_sent:
            try:
                producer.produce(topic, key=str(i), value=str(timestamp))
                message_sent = True
            except BufferError:
                print("队列已满，等待...")
                producer.poll(1)  # 等待队列有空间

    producer.flush()  # 确保所有消息都被发送
    print("✅ 生产者完成消息发送")
