# main.py
import time
from multiprocessing import Process, Manager
import argparse
import requests
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
            f.result()
            print(f"✅ Topic {topic} 删除成功")
        except Exception as e:
            print(f"⚠️ Topic {topic} 删除失败: {e}")
    print()
    while True:
        topic_metadata = admin_client.list_topics(timeout=10)
        if topic_name not in topic_metadata.topics:
            break
        print(f"⚠️ Topic {topic_name} 仍在删除中，等待...")
    time.sleep(5)

def create_topic(admin_client, topic_name, num_partitions=3, replication_factor=1):
    """
    检查并创建 Kafka 主题，如果已存在则跳过。
    """
    topic_metadata = admin_client.list_topics(timeout=10)
    if topic_name in topic_metadata.topics:
        print(f"⚠️ Topic {topic_name} 已存在，跳过创建")
        return

    print(f"🚀 创建 Topic: {topic_name}")
    topic_list = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    fs = admin_client.create_topics(topic_list, operation_timeout=30)
    for t, f in fs.items():
        try:
            f.result()
            print(f"✅ Topic {t} 创建成功")
        except Exception as e:
            print(f"⚠️ Topic {t} 创建失败: {e}")
    print()

def start_remote_monitoring(remote_ips):
    """
    对每个 Kafka 服务器调用 /start_monitor 接口
    :param remote_ips: list of str, 每个服务器的 IP（或域名）
    """
    for ip in remote_ips:
        url = f"http://{ip}:5000/start_monitor"
        try:
            resp = requests.get(url, timeout=5)
            print(f"远程监控启动[{ip}]: {resp.json()}")
        except Exception as e:
            print(f"启动远程监控[{ip}]失败: {e}")

def stop_remote_monitoring(remote_ips):
    """
    对每个 Kafka 服务器调用 /stop_monitor 接口，返回各服务器的监控数据
    :param remote_ips: list of str, 每个服务器的 IP（或域名）
    :return: list of dict，每个元素包含 avg_cpu、avg_mem 等
    """
    results = []
    for ip in remote_ips:
        url = f"http://{ip}:5000/stop_monitor"
        try:
            resp = requests.get(url, timeout=5)
            data = resp.json()
            data["ip"] = ip
            results.append(data)
            print(f"远程监控停止[{ip}]: {data}")
        except Exception as e:
            print(f"停止远程监控[{ip}]失败: {e}")
    return results

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MQ 吞吐量与延迟测试")
    parser.add_argument("--mq_type", type=str, default="kafka", help="消息队列类型, kafka 或 rabbitmq")
    parser.add_argument("--broker_address", type=str, default="localhost:9092", help="Broker 地址")
    parser.add_argument("--topic", type=str, default="test-throughput", help="测试 Topic 名称")
    parser.add_argument("--num_producers", type=int, default=50, help="生产者数量")
    parser.add_argument("--num_consumers", type=int, default=50, help="消费者数量")
    parser.add_argument("--messages_per_producer", type=int, default=1000, help="每个生产者发送的消息数量")
    parser.add_argument("--log_interval", type=int, default=100, help="日志打印间隔")
    parser.add_argument("--remote_ips", type=str, default="", help="Kafka服务器IP列表，逗号分隔")
    args = parser.parse_args()

    # 解析远程IP列表
    remote_ips = [ip.strip() for ip in args.remote_ips.split(",") if ip.strip()]
    if not remote_ips:
        print("请指定 Kafka 服务器IP列表（--remote_ips），用于采集资源指标")
        exit(1)

    total_messages = args.num_producers * args.messages_per_producer
    messages_per_consumer = total_messages // args.num_consumers

    if args.mq_type.lower() == "kafka":
        admin_client = AdminClient({"bootstrap.servers": args.broker_address})
        delete_topic(admin_client, args.topic)
        # 设置分区数为消费者数量，确保每个消费者分到任务
        create_topic(admin_client, args.topic, num_partitions=args.num_consumers, replication_factor=1)
    else:
        print("目前仅支持 Kafka, 其他 MQ 需要实现对应适配器")
        exit(1)

    # 启动远程 Kafka 服务器的资源监控
    start_remote_monitoring(remote_ips)

    manager = Manager()
    producer_metrics = manager.list()
    consumer_metrics = manager.list()
    processes = []

    # 启动消费者进程
    for i in range(args.num_consumers):
        p = Process(target=consume_messages, args=(
            {
                'bootstrap.servers': args.broker_address,
                'group.id': "latency-test-group",
                'auto.offset.reset': 'earliest'
            },
            args.topic, messages_per_consumer, args.log_interval, consumer_metrics, i
        ))
        p.start()
        processes.append(p)

    # 启动生产者进程
    for i in range(args.num_producers):
        p = Process(target=produce_messages, args=(
            {
                'bootstrap.servers': args.broker_address,
                'acks': 'all',
                'batch.size': 16384,
                'linger.ms': 5,
                'compression.type': 'lz4'
            },
            args.topic, args.messages_per_producer, args.log_interval, producer_metrics, i
        ))
        p.start()
        processes.append(p)

    # 等待所有进程结束
    for p in processes:
        p.join()

    # 停止远程监控，获取各 Kafka 服务器的资源数据
    remote_results = stop_remote_monitoring(remote_ips)
    if remote_results:
        avg_cpu_all = sum(item["avg_cpu"] for item in remote_results) / len(remote_results)
        avg_mem_all = sum(item["avg_mem"] for item in remote_results) / len(remote_results)
        print("\n===== Kafka服务器资源使用情况 =====")
        print(f"三个服务器平均 CPU 占用: {avg_cpu_all:.2f}%")
        print(f"三个服务器平均 内存占用: {avg_mem_all/1024/1024:.2f} MB")
        for item in remote_results:
            print(f"服务器 {item['ip']}： CPU: {item['avg_cpu']:.2f}%, 内存: {item['avg_mem']/1024/1024:.2f} MB (采样 {item.get('samples_count',0)} 次)")
    else:
        print("未获取到远程资源数据。")

    # 汇总生产者、消费者指标（如需要）
    total_messages_produced = sum(item["messages"] for item in producer_metrics)
    max_producer_duration = max(item["duration"] for item in producer_metrics) if producer_metrics else 1
    overall_throughput_producers = total_messages_produced / max_producer_duration

    avg_latency = sum(item["avg_latency"] for item in consumer_metrics) / len(consumer_metrics) if consumer_metrics else 0
    avg_p99_latency = sum(item["p99_latency"] for item in consumer_metrics) / len(consumer_metrics) if consumer_metrics else 0
    avg_cold_start = sum(item["cold_start_latency"] for item in consumer_metrics) / len(consumer_metrics) if consumer_metrics else 0

    print("\n===== 综合测试结果 =====")
    print(f"生产者: 总发送消息数: {total_messages_produced}, 平均吞吐量: {overall_throughput_producers:.2f} msg/s")
    print("消费者:")
    print(f"  平均延迟: {avg_latency:.6f} s")
    print(f"  近似99%延迟: {avg_p99_latency:.6f} s")
    print(f"  平均冷启动延迟: {avg_cold_start:.6f} s")
