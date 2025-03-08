# consumer_rocketmq.py
import time
import utility
from rocketmq.client import PushConsumer

@utility.timer
def consume_messages_rocketmq(consumer_conf, topic, log_interval,
                              metrics_list=None, process_id=0):
    """
    使用“全局总量”退出的 RocketMQ 消费者示例。
    :param consumer_conf: dict, 包含:
        - namesrv_addr: RocketMQ NameServer
        - consumer_group: 消费组ID
        - global_count: 全局已消费数（manager.Value）
        - count_lock: manager.Lock() 保护global_count
        - global_stop: manager.Value(bool)，是否已消费完
        - total_messages: int，总消息数量
    :param topic: 要消费的Topic
    :param log_interval: int, 打印日志间隔
    :param metrics_list: 用于存放指标数据
    :param process_id: 消费者编号
    """
    start_time = time.time()

    # 从 consumer_conf 解析需要的共享对象
    namesrv_addr = consumer_conf.get("namesrv_addr", "localhost:9876")
    group_id = consumer_conf.get("consumer_group", f"CID_TEST_{process_id}")
    global_count = consumer_conf["global_count"]
    count_lock = consumer_conf["count_lock"]
    global_stop = consumer_conf["global_stop"]
    total_messages = consumer_conf["total_messages"]

    # 用于本地统计
    latencies = []
    cold_start_latencies = []
    cold_start_count = 50
    local_count = 0  # 当前消费者自己消费了多少条

    consumer = PushConsumer(group_id)
    consumer.set_name_server_address(namesrv_addr)

    # 回调函数：每收到一条消息都会调用
    def callback(msg):
        nonlocal local_count
        if global_stop.value:
            # 若全局标志已经是 True，说明够了，后续消息可直接忽略
            return 0

        current_time = time.time()
        try:
            body_str = msg.body.decode('utf-8')
            sent_str = body_str.split('|')[0]
            sent_ts = float(sent_str)
        except Exception as e:
            print(f"🔴 [RocketMQ]消费者[{process_id}]解码失败: {e}")
            return 0

        latency = current_time - sent_ts
        latencies.append(latency)
        if local_count < cold_start_count:
            cold_start_latencies.append(latency)

        # 本地计数 + 1
        local_count += 1

        # 打印本地计数日志
        if local_count % log_interval == 0:
            print(f"🔴 [RocketMQ]消费者[{process_id}]接收消息: local_count={local_count}")

        # 全局计数 + 1
        with count_lock:
            global_count.value += 1
            # 如果全局已满足 total_messages，就设置停止标志
            if global_count.value >= total_messages:
                global_stop.value = True

        return 0

    # 订阅 & 启动消费者
    consumer.subscribe(topic, callback)
    consumer.start()

    # 主循环：只要没到 global_stop 就一直 sleep
    while True:
        # 如果达到总量就退出
        if global_stop.value:
            break
        time.sleep(0.2)

    # 关闭 consumer
    consumer.shutdown()
    end_time = time.time()
    duration = end_time - start_time

    # 由于可能有多个消费者，实际消费到的条数 = local_count
    # 吞吐量计算基于本地消费数
    throughput = local_count / duration if duration > 0 else 0

    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    sorted_lat = sorted(latencies)
    p99_latency = sorted_lat[int(len(sorted_lat) * 0.99) - 1] if len(sorted_lat) > 0 else 0
    max_latency = max(latencies) if latencies else 0
    cold_start_latency = sum(cold_start_latencies) / len(cold_start_latencies) if cold_start_latencies else 0

    metrics = {
        "process_id": process_id,
        "role": "consumer",
        "messages": local_count,  # 实际消费到的数量
        "duration": duration,
        "throughput": throughput,
        "avg_latency": avg_latency,
        "p99_latency": p99_latency,
        "max_latency": max_latency,
        "cold_start_latency": cold_start_latency,
    }
    if metrics_list is not None:
        metrics_list.append(metrics)

    print(f"✅ [RocketMQ]消费者[{process_id}]结束, 共消费{local_count}条, 用时{duration:.2f}s, 吞吐量{throughput:.2f} msg/s")
