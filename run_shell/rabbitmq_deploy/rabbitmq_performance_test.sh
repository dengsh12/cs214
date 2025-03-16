#!/bin/bash

# RabbitMQ节点信息
RABBITMQ_NODES=( "rabbitmq-1;10.128.0.12;34.59.119.117" "rabbitmq-2;10.128.0.10;34.68.113.17" "rabbitmq-3;10.128.0.11;35.238.173.156" )
ZONE="us-central1-c"

# 测试参数
MESSAGE_SIZE=100  # 单位：字节
NUM_MESSAGES=100000  # 总消息数
NUM_PRODUCERS=6  # 生产者数量
NUM_CONSUMERS=6  # 消费者数量
TEST_DURATION=60  # 测试持续时间(秒)
QUEUE_NAME="perf_test_queue"

echo "========== RabbitMQ集群性能测试 =========="
echo "消息大小: ${MESSAGE_SIZE}字节"
echo "测试消息数: ${NUM_MESSAGES}"
echo "生产者数量: ${NUM_PRODUCERS} (每个节点 $((NUM_PRODUCERS/3)))"
echo "消费者数量: ${NUM_CONSUMERS} (每个节点 $((NUM_CONSUMERS/3)))"
echo "测试持续时间: ${TEST_DURATION}秒"
echo "负载均衡模式: 启用"

# 安装必要的依赖
echo "安装测试依赖..."
sudo apt-get update -y
sudo apt-get install -y python3-pip sysstat jq
pip3 install pika statistics numpy pandas

# 创建测试脚本
echo "创建测试脚本..."

cat > rabbitmq_load_test.py << 'EOL'
#!/usr/bin/env python3

import pika
import time
import threading
import random
import string
import json
import statistics
import numpy as np
import argparse
import subprocess
import os
import signal
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from collections import deque

# 解析命令行参数
parser = argparse.ArgumentParser(description='RabbitMQ性能测试')
parser.add_argument('--size', type=int, default=100, help='消息大小(字节)')
parser.add_argument('--count', type=int, default=100000, help='消息数量')
parser.add_argument('--producers', type=int, default=6, help='生产者数量')
parser.add_argument('--consumers', type=int, default=6, help='消费者数量')
parser.add_argument('--duration', type=int, default=60, help='测试持续时间(秒)')
parser.add_argument('--queue', type=str, default='perf_test_queue', help='队列名称')
parser.add_argument('--load-balance', action='store_true', default=True, help='启用负载均衡')
args = parser.parse_args()

# RabbitMQ节点信息
RABBITMQ_NODES = [
    {"name": "rabbitmq-1", "internal_ip": "10.128.0.12", "external_ip": "34.59.119.117"},
    {"name": "rabbitmq-2", "internal_ip": "10.128.0.10", "external_ip": "34.68.113.17"},
    {"name": "rabbitmq-3", "internal_ip": "10.128.0.11", "external_ip": "35.238.173.156"}
]
ZONE = "us-central1-c"

# 共享数据
class SharedData:
    def __init__(self):
        self.produced_count = 0
        self.consumed_count = 0
        self.producer_start_time = None
        self.producer_end_time = None
        self.consumer_start_time = None
        self.consumer_end_time = None
        self.consumer_latencies = []
        self.producer_stop = False
        self.consumer_stop = False
        self.lock = threading.Lock()
        self.server_stats = {node["name"]: {"cpu": [], "memory": []} for node in RABBITMQ_NODES}
        # 每个节点的消息统计
        self.node_stats = {node["name"]: {"produced": 0, "consumed": 0} for node in RABBITMQ_NODES}

shared_data = SharedData()

# 负载均衡器 - 简单轮询算法
class LoadBalancer:
    def __init__(self, nodes):
        self.nodes = nodes
        self.producer_index = 0
        self.consumer_index = 0
        self.lock = threading.Lock()
        
    def get_next_producer_node(self):
        with self.lock:
            node = self.nodes[self.producer_index % len(self.nodes)]
            self.producer_index += 1
            return node
            
    def get_next_consumer_node(self):
        with self.lock:
            node = self.nodes[self.consumer_index % len(self.nodes)]
            self.consumer_index += 1
            return node

load_balancer = LoadBalancer(RABBITMQ_NODES)

# 用于收集服务器资源使用情况
def monitor_server_resources():
    print("开始监控服务器资源使用情况...")
    
    while not (shared_data.producer_stop and shared_data.consumer_stop):
        for node in RABBITMQ_NODES:
            try:
                # 获取CPU使用率
                cmd = f"gcloud compute ssh {node['name']} --zone={ZONE} --command=\"top -b -n1 | grep 'Cpu(s)' | awk '{{print \\$2+\\$4}}'\""
                cpu_output = subprocess.check_output(cmd, shell=True).decode().strip()
                cpu_usage = float(cpu_output)
                
                # 获取内存使用情况(MB)
                cmd = f"gcloud compute ssh {node['name']} --zone={ZONE} --command=\"free -m | grep Mem | awk '{{print \\$3}}'\""
                mem_output = subprocess.check_output(cmd, shell=True).decode().strip()
                mem_usage = float(mem_output)
                
                with shared_data.lock:
                    shared_data.server_stats[node["name"]]["cpu"].append(cpu_usage)
                    shared_data.server_stats[node["name"]]["memory"].append(mem_usage)
                    
            except Exception as e:
                print(f"监控节点 {node['name']} 时出错: {str(e)}")
                
        time.sleep(1)
    
    print("服务器资源监控完成")

# 生成随机消息
def generate_message(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

# 生产者函数
def producer(producer_id, message_size, message_count):
    # 负载均衡 - 获取节点
    if args.load_balance:
        node = load_balancer.get_next_producer_node()
        host = node["external_ip"]
        node_name = node["name"]
        print(f"生产者 {producer_id} 连接到节点 {node_name} ({host})")
    else:
        # 默认使用第一个节点
        host = RABBITMQ_NODES[0]["external_ip"]
        node_name = RABBITMQ_NODES[0]["name"]
        print(f"生产者 {producer_id} 连接到节点 {node_name} ({host})")
    
    try:
        # 创建连接
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, heartbeat=600)
        )
        channel = connection.channel()
        
        # 声明队列
        channel.queue_declare(queue=args.queue, durable=True)
        
        message_payload = generate_message(message_size)
        
        if producer_id == 0:  # 只在第一个生产者中设置开始时间
            with shared_data.lock:
                shared_data.producer_start_time = time.time()
        
        local_count = 0
        while not shared_data.producer_stop:
            # 添加时间戳到消息中
            message = {
                "payload": message_payload,
                "timestamp": time.time() * 1000,  # 毫秒级时间戳
                "producer_id": producer_id,
                "node": node_name
            }
            
            # 发送消息
            channel.basic_publish(
                exchange='',
                routing_key=args.queue,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 消息持久化
                )
            )
            
            local_count += 1
            
            with shared_data.lock:
                shared_data.produced_count += 1
                shared_data.node_stats[node_name]["produced"] += 1
                if shared_data.produced_count >= message_count:
                    shared_data.producer_stop = True
        
        # 记录结束时间            
        with shared_data.lock:
            if shared_data.producer_end_time is None:
                shared_data.producer_end_time = time.time()
                
        # 关闭连接
        connection.close()
        print(f"生产者 {producer_id} 完成，发送了 {local_count} 条消息到 {node_name}")
        
    except Exception as e:
        print(f"生产者 {producer_id} 发生错误: {str(e)}")

# 消费者回调函数
def on_message(channel, method, properties, body, consumer_id, node_name):
    try:
        # 解析消息
        message = json.loads(body)
        send_timestamp = message["timestamp"]
        source_node = message.get("node", "unknown")
        
        # 计算延迟
        receive_timestamp = time.time() * 1000
        latency = receive_timestamp - send_timestamp
        
        with shared_data.lock:
            # 记录第一条消息的时间
            if shared_data.consumer_start_time is None:
                shared_data.consumer_start_time = time.time()
                
            # 添加延迟数据
            shared_data.consumer_latencies.append(latency)
            
            # 增加消费计数
            shared_data.consumed_count += 1
            shared_data.node_stats[node_name]["consumed"] += 1
            
            # 检查是否应该停止消费
            if shared_data.consumed_count >= args.count or shared_data.producer_stop:
                if shared_data.consumer_end_time is None:
                    shared_data.consumer_end_time = time.time()
        
        # 确认消息
        channel.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"处理消息时发生错误: {str(e)}")
        channel.basic_nack(delivery_tag=method.delivery_tag)

# 消费者函数
def consumer(consumer_id):
    # 负载均衡 - 获取节点
    if args.load_balance:
        node = load_balancer.get_next_consumer_node()
        host = node["external_ip"]
        node_name = node["name"]
        print(f"消费者 {consumer_id} 连接到节点 {node_name} ({host})")
    else:
        # 默认使用第一个节点
        host = RABBITMQ_NODES[0]["external_ip"]
        node_name = RABBITMQ_NODES[0]["name"]
        print(f"消费者 {consumer_id} 连接到节点 {node_name} ({host})")
    
    try:
        # 创建连接
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, heartbeat=600)
        )
        channel = connection.channel()
        
        # 声明队列
        channel.queue_declare(queue=args.queue, durable=True)
        
        # 设置QoS
        channel.basic_qos(prefetch_count=10)
        
        # 设置消费回调，捕获节点名称
        callback = lambda ch, method, properties, body: on_message(
            ch, method, properties, body, consumer_id, node_name
        )
        
        # 设置消费
        channel.basic_consume(
            queue=args.queue,
            on_message_callback=callback
        )
        
        # 开始消费
        print(f"消费者 {consumer_id} 开始消费来自 {node_name} 的消息")
        
        local_count = 0
        # 消费直到停止信号
        while not shared_data.consumer_stop:
            connection.process_data_events(time_limit=1)
            
            # 强制检查是否应该停止 (在消息量不足的情况下)
            if shared_data.producer_stop and shared_data.consumed_count >= shared_data.produced_count:
                with shared_data.lock:
                    if shared_data.consumer_end_time is None:
                        shared_data.consumer_end_time = time.time()
                    shared_data.consumer_stop = True
        
        # 关闭连接
        connection.close()
        print(f"消费者 {consumer_id} 完成")
        
    except Exception as e:
        print(f"消费者 {consumer_id} 发生错误: {str(e)}")

# 测试协调函数
def run_test():
    # 创建连接以清理队列
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_NODES[0]["external_ip"])
    )
    channel = connection.channel()
    
    # 删除可能存在的旧队列
    channel.queue_delete(queue=args.queue)
    
    # 创建新队列
    channel.queue_declare(queue=args.queue, durable=True)
    connection.close()
    
    # 使用线程池启动监控、生产者和消费者
    with ThreadPoolExecutor(max_workers=args.producers + args.consumers + 1) as executor:
        # 启动服务器资源监控
        monitor_thread = executor.submit(monitor_server_resources)
        
        # 启动消费者
        consumer_threads = [
            executor.submit(consumer, i)
            for i in range(args.consumers)
        ]
        
        # 延迟一下，确保消费者准备好
        time.sleep(2)
        
        # 启动生产者
        producer_threads = [
            executor.submit(producer, i, args.size, args.count)
            for i in range(args.producers)
        ]
        
        # 设置超时
        start_time = time.time()
        while not (shared_data.producer_stop and shared_data.consumer_stop):
            time.sleep(1)
            elapsed = time.time() - start_time
            
            # 显示实时进度
            if elapsed % 5 == 0:  # 每5秒显示一次
                with shared_data.lock:
                    if shared_data.produced_count > 0:
                        print(f"进度: 已产生 {shared_data.produced_count} 消息, 已消费 {shared_data.consumed_count} 消息")
                
            # 如果超过指定时间，强制结束测试
            if elapsed > args.duration:
                print(f"测试达到最大持续时间 {args.duration} 秒，强制结束")
                shared_data.producer_stop = True
                
                # 如果所有消息都已经消费完，或者超时10秒，也结束消费者
                if shared_data.consumed_count >= shared_data.produced_count or elapsed > args.duration + 10:
                    with shared_data.lock:
                        if shared_data.consumer_end_time is None:
                            shared_data.consumer_end_time = time.time()
                    shared_data.consumer_stop = True
                break
        
        # 等待所有线程完成
        print("等待所有线程完成...")
        for future in producer_threads + consumer_threads:
            future.result()

    # 计算并返回结果
    return calculate_results()

# 计算测试结果
def calculate_results():
    results = {}
    
    # 计算总生产消息数
    results["total_messages_produced"] = shared_data.produced_count
    results["total_messages_consumed"] = shared_data.consumed_count
    
    # 计算生产者吞吐量 (消息/秒)
    if shared_data.producer_start_time and shared_data.producer_end_time:
        producer_duration = shared_data.producer_end_time - shared_data.producer_start_time
        results["producer_duration"] = producer_duration
        results["producer_avg_throughput"] = shared_data.produced_count / producer_duration if producer_duration > 0 else 0
    else:
        results["producer_duration"] = 0
        results["producer_avg_throughput"] = 0
    
    # 计算消费者吞吐量 (消息/秒)
    if shared_data.consumer_start_time and shared_data.consumer_end_time:
        consumer_duration = shared_data.consumer_end_time - shared_data.consumer_start_time
        results["consumer_duration"] = consumer_duration
        results["consumer_avg_throughput"] = shared_data.consumed_count / consumer_duration if consumer_duration > 0 and shared_data.consumed_count > 0 else 0
    else:
        results["consumer_duration"] = 0
        results["consumer_avg_throughput"] = 0
    
    # 计算消费者延迟 (毫秒)
    if shared_data.consumer_latencies:
        results["consumer_avg_latency"] = statistics.mean(shared_data.consumer_latencies)
        results["consumer_p99_latency"] = np.percentile(shared_data.consumer_latencies, 99)
    else:
        results["consumer_avg_latency"] = 0
        results["consumer_p99_latency"] = 0
    
    # 节点统计
    results["node_stats"] = shared_data.node_stats
    
    # 计算服务器资源使用情况
    remote_cpu_values = []
    remote_mem_values = []
    remote_servers = []
    
    for node_name, stats in shared_data.server_stats.items():
        node_cpu = statistics.mean(stats["cpu"]) if stats["cpu"] else 0
        node_mem = statistics.mean(stats["memory"]) if stats["memory"] else 0
        
        remote_cpu_values.append(node_cpu)
        remote_mem_values.append(node_mem)
        
        node_produced = shared_data.node_stats[node_name]["produced"]
        node_consumed = shared_data.node_stats[node_name]["consumed"]
        
        remote_servers.append({
            "name": node_name,
            "avg_cpu": node_cpu,
            "avg_mem_mb": node_mem,
            "messages_produced": node_produced,
            "messages_consumed": node_consumed
        })
    
    results["remote_avg_cpu"] = statistics.mean(remote_cpu_values) if remote_cpu_values else 0
    results["remote_avg_mem_mb"] = statistics.mean(remote_mem_values) if remote_mem_values else 0
    results["remote_servers"] = remote_servers
    
    return results

# 主函数
def main():
    # 捕获SIGINT信号
    def signal_handler(sig, frame):
        print("接收到中断信号，正在停止测试...")
        shared_data.producer_stop = True
        shared_data.consumer_stop = True
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print(f"开始RabbitMQ性能测试:")
    print(f"- 队列: {args.queue}")
    print(f"- 消息大小: {args.size}字节")
    print(f"- 消息数量: {args.count}")
    print(f"- 生产者数量: {args.producers}")
    print(f"- 消费者数量: {args.consumers}")
    print(f"- 最大测试时间: {args.duration}秒")
    print(f"- 负载均衡模式: {'启用' if args.load_balance else '禁用'}")
    
    # 运行测试
    results = run_test()
    
    # 输出结果
    print("\n========== 测试结果 ==========")
    print(f"总生产消息数: {results['total_messages_produced']}")
    print(f"总消费消息数: {results['total_messages_consumed']}")
    print(f"生产者平均吞吐量: {results['producer_avg_throughput']:.2f} 消息/秒")
    print(f"消费者平均吞吐量: {results['consumer_avg_throughput']:.2f} 消息/秒")
    print(f"消费者平均延迟: {results['consumer_avg_latency']:.2f} 毫秒")
    print(f"消费者P99延迟: {results['consumer_p99_latency']:.2f} 毫秒")
    print(f"集群平均CPU使用率: {results['remote_avg_cpu']:.2f}%")
    print(f"集群平均内存使用量: {results['remote_avg_mem_mb']:.2f} MB")
    
    print("\n节点资源使用情况:")
    for server in results["remote_servers"]:
        print(f"- {server['name']}: CPU {server['avg_cpu']:.2f}%, 内存 {server['avg_mem_mb']:.2f} MB")
        print(f"  消息生产: {server['messages_produced']}, 消息消费: {server['messages_consumed']}")
    
    # 以JSON格式保存结果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"rabbitmq_test_results_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n测试结果已保存到: {filename}")

if __name__ == "__main__":
    main()
EOL

# 使脚本可执行
chmod +x rabbitmq_load_test.py

# 执行测试脚本
echo "开始执行性能测试..."
python3 rabbitmq_load_test.py --size ${MESSAGE_SIZE} --count ${NUM_MESSAGES} --producers ${NUM_PRODUCERS} --consumers ${NUM_CONSUMERS} --duration ${TEST_DURATION} --load-balance 