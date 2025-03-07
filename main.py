import time
from multiprocessing import Process, Manager
import argparse
import requests
from confluent_kafka.admin import AdminClient, NewTopic
from producer import produce_messages
from consumer import consume_messages
import utility
import os
import json

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
    对每个 Kafka 服务器调用 /start_monitor 接口，并保存初始数据
    :param remote_ips: list of str, 每个服务器的 IP（或域名）
    :return: dict，key为ip，value为初始监控数据
    """
    baseline_data = {}
    for ip in remote_ips:
        url = f"http://{ip}:5000/start_monitor"
        try:
            resp = requests.get(url, timeout=5)
            data = resp.json()
            baseline_data[ip] = data
            print(f"远程监控启动[{ip}]: {data}")
        except Exception as e:
            print(f"启动远程监控[{ip}]失败: {e}")
    return baseline_data

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
    parser.add_argument("--message_size", type=int, default=100, help="消息大小（字节）")
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
        # 设置分区数为消费者数量，确保每个消费者都有任务
        create_topic(admin_client, args.topic, num_partitions=args.num_consumers, replication_factor=1)
    else:
        print("目前仅支持 Kafka, 其他 MQ 需要实现对应适配器")
        exit(1)

    # 启动远程 Kafka 服务器的资源监控，并保存初始数据
    baseline_metrics = start_remote_monitoring(remote_ips)

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

    # 启动生产者进程，并传入 message_size 参数
    for i in range(args.num_producers):
        p = Process(target=produce_messages, args=(
            {
                'bootstrap.servers': args.broker_address,
                'acks': 'all',
                'batch.size': 16384,
                'linger.ms': 5,
                'compression.type': 'lz4'
            },
            args.topic, args.messages_per_producer, args.log_interval, producer_metrics, i, args.message_size
        ))
        p.start()
        processes.append(p)

    # 等待所有进程结束
    for p in processes:
        p.join()

    # 停止远程监控，获取各 Kafka 服务器的最终资源数据
    remote_results = stop_remote_monitoring(remote_ips)
    # 计算每个服务器的增量（最终 - 初始），并转换 CPU 为小数、内存转换为 MB
    processed_remote = []
    if remote_results:
        for item in remote_results:
            ip = item["ip"]
            baseline = baseline_metrics.get(ip, {})
            delta_cpu = item.get("avg_cpu", 0) - baseline.get("avg_cpu", 0)
            delta_mem = item.get("avg_mem", 0) - baseline.get("avg_mem", 0)
            processed_item = {
                "ip": ip,
                "avg_cpu": round(delta_cpu / 100.0, 4),  # 转为小数表示
                "avg_mem_mb": round(delta_mem / (1024*1024), 2),
                "samples_count": item.get("samples_count", 0)
            }
            processed_remote.append(processed_item)
        avg_cpu_all = sum(item.get("avg_cpu", 0) - baseline_metrics.get(item["ip"], {}).get("avg_cpu", 0) for item in remote_results) / len(remote_results) / 100.0
        avg_mem_all = sum(item.get("avg_mem", 0) - baseline_metrics.get(item["ip"], {}).get("avg_mem", 0) for item in remote_results) / len(remote_results) / (1024*1024)
        print("\n===== Kafka服务器资源使用情况 =====")
        print(f"三个服务器平均 CPU 增量: {avg_cpu_all:.4f}")
        print(f"三个服务器平均 内存增量: {avg_mem_all:.2f} MB")
        for item in processed_remote:
            print(f"服务器 {item['ip']}： CPU 增量: {item['avg_cpu']:.4f}, 内存增量: {item['avg_mem_mb']:.2f} MB (采样 {item['samples_count']} 次)")
    else:
        print("未获取到远程资源数据。")

    # 汇总生产者、消费者指标
    total_messages_produced = sum(item["messages"] for item in producer_metrics)
    max_producer_duration = max(item["duration"] for item in producer_metrics) if producer_metrics else 1
    overall_throughput_producers = total_messages_produced / max_producer_duration

    # 计算消费者平均吞吐量：每个消费者的吞吐量均值
    avg_consumer_throughput = sum(item["throughput"] for item in consumer_metrics) / len(consumer_metrics) if consumer_metrics else 0
    avg_latency = sum(item["avg_latency"] for item in consumer_metrics) / len(consumer_metrics) if consumer_metrics else 0
    avg_p99_latency = sum(item["p99_latency"] for item in consumer_metrics) / len(consumer_metrics) if consumer_metrics else 0
    avg_cold_start = sum(item["cold_start_latency"] for item in consumer_metrics) / len(consumer_metrics) if consumer_metrics else 0

    print("\n===== 综合测试结果 =====")
    print(f"生产者: 总发送消息数: {total_messages_produced}, 平均吞吐量: {overall_throughput_producers:.2f} msg/s")
    print("消费者:")
    print(f"  平均延迟: {avg_latency:.6f} s")
    print(f"  近似99%延迟: {avg_p99_latency:.6f} s")
    print(f"  平均冷启动延迟: {avg_cold_start:.6f} s")
    print(f"  平均吞吐量: {avg_consumer_throughput:.2f} msg/s")

    # 保存参数和结果到 results 目录下的新文件
    results_summary = {
        "parameters": {
            "mq_type": args.mq_type,
            "broker_address": args.broker_address,
            "topic": args.topic,
            "num_producers": args.num_producers,
            "num_consumers": args.num_consumers,
            "messages_per_producer": args.messages_per_producer,
            "log_interval": args.log_interval,
            "message_size": args.message_size,
            "remote_ips": remote_ips
        },
        "summary": {
            "total_messages_produced": total_messages_produced,
            "producer_avg_throughput": overall_throughput_producers,
            "consumer_avg_throughput": avg_consumer_throughput,
            "consumer_avg_latency": avg_latency,
            "consumer_p99_latency": avg_p99_latency,
            "consumer_cold_start_latency": avg_cold_start,
            "remote_servers": processed_remote,
            "remote_avg_cpu": round(avg_cpu_all, 4),
            "remote_avg_mem_mb": round(avg_mem_all, 2)
        }
    }
    results_dir = os.path.join("results", args.mq_type)
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = os.path.join(results_dir, f"{args.num_consumers}consumer{args.num_producers}producer{args.messages_per_producer//1000}kmessages_{timestamp}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results_summary, f, indent=4, ensure_ascii=False)
    print(f"\n结果已保存至 {filename}")
