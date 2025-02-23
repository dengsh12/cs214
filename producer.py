# producer.py
import time
from confluent_kafka import Producer
import utility

@utility.timer
def produce_messages(producer_conf, topic, num_messages, log_interval, metrics_list=None, process_id=0, message_size=100):
    """
    生产者发送带有时间戳的消息，同时监控资源使用和记录指标
    :param producer_conf: dict, Producer 配置信息
    :param topic: str, 发送到的 Kafka Topic
    :param num_messages: int, 发送的消息数
    :param log_interval: int, 日志打印间隔
    :param metrics_list: Manager list, 用于存放指标数据
    :param process_id: int, 生产者进程编号
    :param message_size: int, 消息总大小（字节），如果大于时间戳和分隔符长度，则填充“0”
    """
    # 开启资源监控
    samples, stop_event, monitor_thread = utility.resource_monitor()
    start_time = time.time()
    producer = Producer(producer_conf)

    for i in range(num_messages):
        if i % log_interval == 0:
            print(f"🚀 生产者[{process_id}]发送消息: {i}/{num_messages}")
        timestamp = time.time()
        ts_str = str(timestamp)
        separator = "|"
        # 如果设定的消息大小足够，则补充填充字符 "0"
        if message_size > len(ts_str) + len(separator):
            padding_len = message_size - len(ts_str) - len(separator)
            padding = "0" * padding_len
            payload = ts_str + separator + padding
        else:
            payload = ts_str  # 如果消息大小设定过小，则仅发送时间戳

        message_sent = False
        while not message_sent:
            try:
                # 用 process_id-消息序号作为 key，保证以后可以适配其他 MQ 时做路由
                producer.produce(topic, key=f"{process_id}-{i}", value=payload)
                message_sent = True
            except BufferError:
                print(f"🚀 生产者[{process_id}]队列已满，等待...")
                producer.poll(1)

    producer.flush()
    end_time = time.time()
    duration = end_time - start_time
    throughput = num_messages / duration if duration > 0 else 0
    avg_cpu, avg_mem = utility.stop_resource_monitor(samples, stop_event, monitor_thread)
    metrics = {
        "process_id": process_id,
        "role": "producer",
        "messages": num_messages,
        "duration": duration,
        "throughput": throughput,
        "avg_cpu": avg_cpu,
        "avg_mem": avg_mem,
    }
    if metrics_list is not None:
        metrics_list.append(metrics)
    print(f"✅ 生产者[{process_id}]完成消息发送, 耗时: {duration:.6f} 秒, 吞吐量: {throughput:.2f} msg/s")
