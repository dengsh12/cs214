# consumer.py
import time
from confluent_kafka import Consumer, KafkaException
import utility

@utility.timer
def consume_messages(consumer_conf, topic, num_messages, log_interval, metrics_list=None, process_id=0):
    """
    消费者接收消息，计算延迟，并监控资源使用及冷启动延迟
    :param consumer_conf: dict, Consumer 配置信息
    :param topic: str, 消费的 Kafka Topic
    :param num_messages: int, 消费的消息数
    :param log_interval: int, 日志打印间隔
    :param metrics_list: Manager list, 用于存放指标数据
    :param process_id: int, 消费者进程编号
    :return: latencies list（仅用于内部统计，不作为返回值传递）
    """
    # 开启资源监控
    samples, stop_event, monitor_thread = utility.resource_monitor()
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    latencies = []
    cold_start_latency = None
    count = 0
    start_time = time.time()
    first_msg_time = None

    while count < num_messages:
        if count % log_interval == 0:
            print(f"🔴 消费者[{process_id}]接收消息: {count}/{num_messages}")
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        current_time = time.time()
        if first_msg_time is None:
            first_msg_time = current_time
            cold_start_latency = first_msg_time - start_time
        try:
            sent_time = float(msg.value().decode('utf-8'))
        except Exception as e:
            print(f"🔴 消费者[{process_id}]解码消息失败: {e}")
            continue
        latency = current_time - sent_time
        latencies.append(latency)
        count += 1

    consumer.close()
    end_time = time.time()
    duration = end_time - start_time
    throughput = num_messages / duration if duration > 0 else 0
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    sorted_latencies = sorted(latencies)
    p99_latency = sorted_latencies[int(len(sorted_latencies) * 0.99) - 1] if latencies and len(sorted_latencies) > 0 else 0
    max_latency = max(latencies) if latencies else 0
    avg_cpu, avg_mem = utility.stop_resource_monitor(samples, stop_event, monitor_thread)
    metrics = {
        "process_id": process_id,
        "role": "consumer",
        "messages": num_messages,
        "duration": duration,
        "throughput": throughput,
        "avg_latency": avg_latency,
        "p99_latency": p99_latency,
        "max_latency": max_latency,
        "cold_start_latency": cold_start_latency,
        "avg_cpu": avg_cpu,
        "avg_mem": avg_mem,
    }
    if metrics_list is not None:
        metrics_list.append(metrics)
    print(f"✅ 消费者[{process_id}]完成接收消息, 耗时: {duration:.6f} 秒, 吞吐量: {throughput:.2f} msg/s")
    return latencies
