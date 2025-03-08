# producer_rocketmq.py
import time
import utility
from rocketmq.client import Producer, Message

@utility.timer
def produce_messages_rocketmq(producer_conf, topic, num_messages, log_interval, metrics_list=None, process_id=0, message_size=100):
    """
    RocketMQ 生产者逻辑，和 Kafka 的 produce_messages 保持相同的函数签名 & 返回格式。
    :param producer_conf: dict, 这里将包含 'namesrv_addr' 等 RocketMQ 配置信息
    :param topic: str, RocketMQ 的 Topic 名称
    :param num_messages: int, 发送的消息数量
    :param log_interval: int, 日志打印间隔
    :param metrics_list: 进程间共享的 metrics 列表
    :param process_id: int, 生产者编号
    :param message_size: int, 消息大小（字节）
    """
    samples, stop_event, monitor_thread = utility.resource_monitor()
    start_time = time.time()

    # 初始化 RocketMQ Producer
    namesrv_addr = producer_conf.get("namesrv_addr", "localhost:9876")
    group_id = producer_conf.get("producer_group", f"PID_TEST_{process_id}")
    producer = Producer(group_id)
    producer.set_name_server_address(namesrv_addr)
    producer.start()

    for i in range(num_messages):
        if i % log_interval == 0:
            print(f"🚀 [RocketMQ]生产者[{process_id}]发送消息: {i}/{num_messages}")
        timestamp = time.time()
        ts_str = str(timestamp)
        separator = "|"
        if message_size > len(ts_str) + len(separator):
            padding_len = message_size - len(ts_str) - len(separator)
            padding = "0" * padding_len
            payload = ts_str + separator + padding
        else:
            payload = ts_str

        msg = Message(topic)
        # key 相当于 kafka 中的 message key
        msg.set_keys(f"{process_id}-{i}")
        msg.set_body(payload)

        # 同步发送
        while True:
            try:
                producer.send_sync(msg)
                break
            except Exception as e:
                # 可能出现超时或连接池满等情况，稍等后重试
                print(f"🚀 [RocketMQ]生产者[{process_id}]发送异常: {e}, 重试中...")
                time.sleep(1)

    # 关闭 producer
    producer.shutdown()
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
    print(f"✅ [RocketMQ]生产者[{process_id}]完成消息发送, 耗时: {duration:.6f} 秒, 吞吐量: {throughput:.2f} msg/s")
