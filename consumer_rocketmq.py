import time
import threading
import utility
from rocketmq.client import PushConsumer, ConsumeStatus

@utility.timer
def consume_messages_rocketmq(consumer_conf, topic, log_interval,
                              metrics_list=None, process_id=0):
    """
    使用“全局总量”退出的 RocketMQ 消费者示例（线程安全版本）。
    """
    start_time = time.time()

    # 从 consumer_conf 解析需要的共享对象
    namesrv_addr = consumer_conf.get("namesrv_addr", "localhost:9876")
    group_id = consumer_conf.get("consumer_group", f"CID_TEST_{process_id}")
    global_count = consumer_conf["global_count"]
    count_lock = consumer_conf["count_lock"]
    global_stop = consumer_conf["global_stop"]
    total_messages = consumer_conf["total_messages"]

    # 用于本地统计（需要线程安全）
    latencies = []
    cold_start_latencies = []
    cold_start_count = 50
    local_count = 0  # 当前消费者自己消费的消息数
    local_lock = threading.Lock()  # 保护local_count和列表的锁

    consumer = PushConsumer(group_id)
    consumer.set_name_server_address(namesrv_addr)

    def on_message(msg):
        # 如果全局停止标志已置位，就不处理消息了
        if global_stop.value:
            return ConsumeStatus.CONSUME_SUCCESS

        nonlocal local_count
        current_time = time.time()
        try:
            body_str = msg.body.decode('utf-8')
            sent_str = body_str.split('|')[0]
            sent_ts = float(sent_str)
        except Exception as e:
            print(f"🔴 [RocketMQ]消费者[{process_id}]解码失败: {e}")
            return ConsumeStatus.CONSUME_SUCCESS

        # 计算延迟
        latency = current_time - sent_ts

        # 使用本地锁保护局部统计数据
        latencies.append(latency)
        if local_count < cold_start_count:
            cold_start_latencies.append(latency)
        local_count += 1
        if local_count % log_interval == 0:
            print(f"🔴 [RocketMQ]消费者[{process_id}]接收消息: local_count={local_count}")

        # 全局计数 + 判断是否到达停止条件
        if global_count.value < total_messages:
            global_count.value += 1
            if global_count.value >= total_messages:
                global_stop.value = True

        return ConsumeStatus.CONSUME_SUCCESS

    # 订阅主题
    consumer.subscribe(topic, callback=on_message, expression="*")
    consumer.set_thread_count(1)
    consumer.start()

    # 主循环：只要未全局停止就一直等
    while not global_stop.value:
        time.sleep(0.2)

    consumer.shutdown()
    end_time = time.time()
    duration = end_time - start_time

    # 统计数据（读取时也建议加锁，虽然程序已结束消息处理线程，但这里使用local_lock确保数据完整性）
    with local_lock:
        throughput = local_count / duration if duration > 0 else 0
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        sorted_lat = sorted(latencies)
        p99_latency = sorted_lat[int(len(sorted_lat) * 0.99) - 1] if sorted_lat else 0
        max_latency = max(latencies) if latencies else 0
        cold_start_latency = (sum(cold_start_latencies) / len(cold_start_latencies)
                              if cold_start_latencies else 0)

    if metrics_list is not None:
        metrics_list.append({
            "process_id": process_id,
            "role": "consumer",
            "messages": local_count,
            "duration": duration,
            "throughput": throughput,
            "avg_latency": avg_latency,
            "p99_latency": p99_latency,
            "max_latency": max_latency,
            "cold_start_latency": cold_start_latency,
        })

    print(f"✅ [RocketMQ]消费者[{process_id}]结束, 共消费 {local_count} 条, 用时 {duration:.2f} s, 吞吐量 {throughput:.2f} msg/s")
    print(f"⏱️ consume_messages_rocketmq 执行耗时: {duration:.6f} 秒")
