# consumer.py
import time
from confluent_kafka import Consumer, KafkaException
import utility

@utility.timer
def consume_messages(consumer_conf, topic, num_messages, log_interval, metrics_list=None, process_id=0):
    """
    消费者接收消息，计算延迟
    :param consumer_conf: dict, Consumer 配置信息
    :param topic: str, 消费的 Kafka Topic
    :param num_messages: int, 消费的消息数
    :param log_interval: int, 日志打印间隔
    :param metrics_list: Manager list, 用于存放指标数据
    :param process_id: int, 消费者进程编号
    :return: latencies list（仅用于内部统计）
    """
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    latencies = []
    cold_start_latencies = []  # 用于存放前 N 条消息延迟
    cold_start_count = 50  # 定义前50条消息作为冷启动计算
    count = 0
    start_time = time.time()
    t2latencies = {}

    while count < num_messages:
        if count % log_interval == 0:
            print(f"🔴 消费者[{process_id}]接收消息: {count}/{num_messages}")
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        current_time = time.time()
        try:
            value_str = msg.value().decode('utf-8')
            # 取消息中 "|" 前面的部分作为时间戳
            timestamp_str = value_str.split("|")[0]
            sent_time = float(timestamp_str)
        except Exception as e:
            print(f"🔴 消费者[{process_id}]解码消息失败: {e}")
            continue
        latency = current_time - sent_time
        latencies.append(latency)
        if current_time in t2latencies.keys():
            t2latencies[current_time] = ((t2latencies[current_time][0] * t2latencies[current_time][1] + latency) / (t2latencies[current_time][1] + 1),
                                          t2latencies[current_time][1] + 1)
        else:
            t2latencies[current_time] = (latency, 1)
        
        if count < cold_start_count:
            cold_start_latencies.append(latency)
        count += 1

    consumer.close()
    end_time = time.time()
    duration = end_time - start_time
    throughput = num_messages / duration if duration > 0 else 0
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    sorted_latencies = sorted(latencies)
    p99_latency = sorted_latencies[int(len(sorted_latencies) * 0.99) - 1] if latencies and len(sorted_latencies) > 0 else 0
    max_latency = max(latencies) if latencies else 0
    # 计算冷启动延迟为前 cold_start_count 条消息的平均延迟
    cold_start_latency = sum(cold_start_latencies) / len(cold_start_latencies) if cold_start_latencies else 0
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
        "t2latencies": t2latencies,
    }
    if metrics_list is not None:
        metrics_list.append(metrics)
    print(f"✅ 消费者[{process_id}]完成接收消息, 耗时: {duration:.6f} 秒, 吞吐量: {throughput:.2f} msg/s")
    return latencies
