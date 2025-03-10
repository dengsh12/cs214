import time
import utility
from rocketmq.client import Producer, Message
from utility import logPrint

@utility.timer
def produce_messages_rocketmq(
    producer_conf,
    topic,
    num_messages,
    log_interval,
    metrics_list=None,
    process_id=0,
    message_size=100
):
    logPrint(f"produce_messages_rocketmq {process_id} called")
    # 资源监控
    samples, stop_event, monitor_thread = utility.resource_monitor()
    start_time = time.time()

    # 初始化 Producer
    namesrv_addr = producer_conf.get("namesrv_addr", "localhost:9876")
    group_id = producer_conf.get("producer_group", f"PID_TEST_{process_id}")
    producer = Producer(group_id)
    producer.set_name_server_address(namesrv_addr)
    logPrint(f"before {process_id} producer.start")
    producer.start()
    logPrint("producer.started")

    # 把消息创建、字符串复制拿到外面
    sep = "|"
    timestamp = time.time()
    ts_str = str(timestamp)
    padding_len = message_size - len(ts_str) - len(sep)
    if padding_len>0:
        padding_str = ("0" * padding_len)
    else:
        padding_str = ""

    for i in range(num_messages):
        if i % log_interval == 0:
            logPrint(f"🚀 [RocketMQ]生产者[{process_id}]发送消息: {i}/{num_messages}")

        timestamp = time.time()
        ts_str = str(timestamp)
        if message_size > len(ts_str) + len(sep):
            payload = ts_str + sep + padding_str
        else:
            payload = ts_str

        msg = Message(topic)
        msg.set_keys(f"{process_id}-{i}")
        msg.set_body(payload)

        try:
            producer.send_sync(msg)
        except Exception as e:
            logPrint(f"🚀 [RocketMQ]生产者[{process_id}]发送异常: {e}")

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
    logPrint(
        f"✅ [RocketMQ]生产者[{process_id}]发送完成, "
        f"耗时: {duration:.6f} 秒, 吞吐量: {throughput:.2f} msg/s"
    )
