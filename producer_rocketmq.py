import time
import utility
from rocketmq.client import Producer, Message

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
    # 资源监控
    samples, stop_event, monitor_thread = utility.resource_monitor()
    start_time = time.time()

    # 初始化 Producer
    namesrv_addr = producer_conf.get("namesrv_addr", "localhost:9876")
    group_id = producer_conf.get("producer_group", f"PID_TEST_{process_id}")
    producer = Producer(group_id)
    producer.set_name_server_address(namesrv_addr)
    producer.start()

    # 类似 "batch.size" + "linger.ms"
    BATCH_SIZE_BYTES = 16384
    BATCH_COUNT_MAX  = 100
    LINGER_MS        = 5

    batch_buffer = []
    batch_size_acc = 0
    last_flush_ts = time.time()

    def flush_batch():
        nonlocal batch_buffer, batch_size_acc, last_flush_ts
        if not batch_buffer:
            return
        # 由于 Python 客户端不支持一次性发送 list，这里循环逐条发送
        for msg in batch_buffer:
            while True:
                try:
                    producer.send_sync(msg)
                    break
                except Exception as e:
                    print(f"🚀 [RocketMQ]生产者[{process_id}]发送异常: {e}, 重试中...")
                    time.sleep(1)

        batch_buffer.clear()
        batch_size_acc = 0
        last_flush_ts = time.time()

    for i in range(num_messages):
        if i % log_interval == 0:
            print(f"🚀 [RocketMQ]生产者[{process_id}]发送消息: {i}/{num_messages}")

        timestamp = time.time()
        ts_str = str(timestamp)
        sep = "|"
        if message_size > len(ts_str) + len(sep):
            padding_len = message_size - len(ts_str) - len(sep)
            payload = ts_str + sep + ("0" * padding_len)
        else:
            payload = ts_str

        msg = Message(topic)
        msg.set_keys(f"{process_id}-{i}")
        msg.set_body(payload)

        batch_buffer.append(msg)
        batch_size_acc += len(payload)

        now = time.time()
        if (
            batch_size_acc >= BATCH_SIZE_BYTES
            or len(batch_buffer) >= BATCH_COUNT_MAX
            or (now - last_flush_ts) * 1000 >= LINGER_MS
        ):
            flush_batch()

    flush_batch()

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
    print(
        f"✅ [RocketMQ]生产者[{process_id}]批量发送完成, "
        f"耗时: {duration:.6f} 秒, 吞吐量: {throughput:.2f} msg/s"
    )
