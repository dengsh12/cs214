import time
import threading
import statistics
from confluent_kafka import Producer, Consumer, KafkaException

# Kafka 配置
KAFKA_BROKER = "localhost:9092"
TOPIC = "test-throughput"
NUM_MESSAGES = 100000  # 发送的消息总数

# 配置生产者
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all'
}
producer = Producer(producer_conf)

# 配置消费者
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': "latency-test-group",
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

# 记录吞吐和延迟
latencies = []
start_time = time.time()


def produce_messages():
    """生产者发送带有时间戳的消息"""
    for i in range(NUM_MESSAGES):
        if i % 1000 == 0:
            print(f"🚀 生产者发送消息: {i}/{NUM_MESSAGES}")
        timestamp = time.time()
        producer.produce(TOPIC, key=str(i), value=str(timestamp))
        producer.flush()
        # time.sleep(0.001)  # 控制速率
    print("✅ 生产者完成消息发送")


def consume_messages():
    """消费者接收消息并计算延迟"""
    global start_time
    count = 0
    while count < NUM_MESSAGES:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        sent_time = float(msg.value().decode('utf-8'))
        latency = time.time() - sent_time
        latencies.append(latency)
        count += 1

    consumer.close()
    print("✅ 消费者完成接收消息")


# 启动生产者和消费者
producer_thread = threading.Thread(target=produce_messages)
consumer_thread = threading.Thread(target=consume_messages)

producer_thread.start()
consumer_thread.start()

producer_thread.join()
consumer_thread.join()

# 计算吞吐量和延迟统计
total_time = time.time() - start_time
throughput = NUM_MESSAGES / total_time
avg_latency = statistics.mean(latencies)
p99_latency = statistics.quantiles(latencies, n=100)[98]  # 近似99分位数
max_latency = max(latencies)

print(f"\n📊 测试完成：")
print(f"✅ 发送 {NUM_MESSAGES} 条消息")
print(f"🚀 吞吐量: {throughput:.2f} 条/秒")
print(f"⏳ 平均延迟: {avg_latency:.6f} 秒")
print(f"🔴 99% 延迟: {p99_latency:.6f} 秒")
print(f"⚡ 最大延迟: {max_latency:.6f} 秒")
