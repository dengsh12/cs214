# main.py
from multiprocessing import Process
import time
import threading
from threading import Thread
import statistics
import argparse
from confluent_kafka.admin import AdminClient, NewTopic
from producer import produce_messages
from consumer import consume_messages
import utility

def delete_topic(admin_client, topic_name):
    """删除 Kafka 主题"""
    print(f"尝试删除 Topic: {topic_name}")
    topic_metadata = admin_client.list_topics(timeout=10)
    if topic_name not in topic_metadata.topics:
        print(f"Topic {topic_name} 不存在，跳过删除")
        return
    fs = admin_client.delete_topics([topic_name], operation_timeout=30)

    for topic, f in fs.items():
        try:
            print(f"等待 Topic {topic} 删除")
            f.result()  # 阻塞直到删除完成
            print(f"✅ Topic {topic} 删除成功")
        except Exception as e:
            print(f"⚠️ Topic {topic} 删除失败: {e}")
    print()

    # 等待主题完全删除
    while True:
        topic_metadata = admin_client.list_topics(timeout=10)
        if topic_name not in topic_metadata.topics:
            break
        print(f"⚠️ Topic {topic_name} 仍在删除中，等待...")
    time.sleep(5)  # 等待5秒

def create_topic(admin_client, topic_name, num_partitions=3, replication_factor=1):
    """
    检查并创建 Kafka 主题，如果已经存在则跳过。
    :param admin_client: AdminClient 实例
    :param topic_name: 要创建的 Topic 名称
    :param num_partitions: 分区数
    :param replication_factor: 副本因子
    """
    # 1. 先检查 Topic 是否已存在
    topic_metadata = admin_client.list_topics(timeout=10)
    if topic_name in topic_metadata.topics:
        print(f"⚠️ Topic {topic_name} 已存在，跳过创建")
        return

    # 2. 如果不存在，创建 Topic
    print(f"🚀 创建 Topic: {topic_name}")
    topic_list = [
        NewTopic(topic_name, 
                 num_partitions=num_partitions, 
                 replication_factor=replication_factor)
    ]

    fs = admin_client.create_topics(topic_list, operation_timeout=30)
    for t, f in fs.items():
        try:
            f.result()  # 阻塞直到创建完成或报错
            print(f"✅ Topic {t} 创建成功")
        except Exception as e:
            print(f"⚠️ Topic {t} 创建失败: {e}")
    print()

if __name__ == '__main__':
    # 1. 解析命令行参数
    parser = argparse.ArgumentParser(description="Kafka 吞吐量测试")
    parser.add_argument("--batch_size", type=int, default=16384, help="生产者批处理大小 (bytes)")
    parser.add_argument("--linger_ms", type=int, default=5, help="生产者 linger.ms (ms)")
    parser.add_argument("--broker_address", type=str, default="localhost:9092", help="Kafka broker 地址")
    parser.add_argument("--num_message", type=int, default=2000, help="发送的消息总数")

    args = parser.parse_args()

    # 2. Kafka 基本配置
    KAFKA_BROKER = args.broker_address
    TOPIC = "test-throughput"
    NUM_MESSAGES = args.num_message
    # LOG_INTERVAL = NUM_MESSAGES // 100 if NUM_MESSAGES >= 100 else 1
    LOG_INTERVAL = NUM_MESSAGES // 10 if NUM_MESSAGES >= 100 else 1

    # 3. AdminClient，用于创建/删除 Topic
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})

    # 打印是本地还是云环境
    if not KAFKA_BROKER.startswith("localhost"):
        print("🚀 云环境 Kafka Broker 地址:", KAFKA_BROKER)
    else:
        print("🚀 本地环境 Kafka Broker 地址:", KAFKA_BROKER)

    # 4. 删除并重新创建测试 Topic
    delete_topic(admin_client, TOPIC)
    create_topic(admin_client, TOPIC)

    # 5. 配置生产者 & 消费者
    producer_conf = {
        'compression.type': 'lz4', # 指定压缩算法
        'bootstrap.servers': KAFKA_BROKER,
        'acks': 'all',
        'batch.size': args.batch_size,
        'linger.ms': args.linger_ms
    }
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': "latency-test-group",
        'auto.offset.reset': 'earliest'
    }

    # 6. 准备收集延迟数据
    latencies = []
    start_time = time.time()

    # 7. 启动生产者线程和消费者进程
    # producer_t = Thread(target=produce_messages, args=(producer_conf, TOPIC, NUM_MESSAGES, LOG_INTERVAL))
    # consumer_t = Thread(target=consume_messages, args=(consumer_conf, TOPIC, NUM_MESSAGES, LOG_INTERVAL, latencies))
    producer_t = Process(target=produce_messages, args=(producer_conf, TOPIC, NUM_MESSAGES, LOG_INTERVAL))
    # consumer_t = Process(target=consume_messages, args=(consumer_conf, TOPIC, NUM_MESSAGES, LOG_INTERVAL, latencies))
    
    producer_t.start()
    # time.sleep(3)  # 让生产者先发一会，再启动消费者
    # consumer_t.start()
    # 这里的latencies处理逻辑还有问题
    latencies = consume_messages(consumer_conf, TOPIC, NUM_MESSAGES, LOG_INTERVAL, latencies)

    producer_t.join()
    # consumer_t.join()

    # 8. 计算吞吐量和延迟统计
    total_time = time.time() - start_time
    throughput = NUM_MESSAGES / total_time

    if latencies:
        import statistics
        avg_latency = statistics.mean(latencies)
        # 近似99分位数
        p99_latency = statistics.quantiles(latencies, n=100)[98]  
        max_latency = max(latencies)

        print("\n📊 测试完成：")
        print(f"✅ 发送 {NUM_MESSAGES} 条消息")
        print(f"🚀 吞吐量: {throughput:.2f} 条/秒")
        print(f"⏳ 平均延迟: {avg_latency:.6f} 秒")
        print(f"🔴 99% 延迟: {p99_latency:.6f} 秒")
        print(f"⚡ 最大延迟: {max_latency:.6f} 秒")
    else:
        print("⚠️ 没有接收到任何消息，无法计算延迟统计。")
