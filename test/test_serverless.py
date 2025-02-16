import requests
import threading
import time
import sys
import os
import concurrent.futures

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utility

@utility.timer
def call_manage_topic(url: str, payload: dict):
    """
    调用 http_trigger_manage_topic，删除并重新创建 Topic
    """
    print("[ManageTopic] Deleting and recreating topic...")
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print(f"[ManageTopic] Success: {response.text}")
    else:
        print(f"[ManageTopic] Failed: {response.text}")

@utility.timer
def call_producer(url: str, payload: dict):
    """
    调用 http_trigger_producer，向 Kafka 发送消息
    """
    print("🚀 [Producer] Producing messages...")
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print(f"[Producer] Success: {response.text}")
    else:
        print(f"[Producer] Failed: {response.text}")

@utility.timer
def call_consumer(url: str, payload: dict):
    """
    调用 http_trigger_consumer，从 Kafka 消费消息并计算延迟
    """
    print("🔴 [Consumer] Consuming messages...")
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        print(f"[Consumer] Success:\n{response.text}")
    else:
        print(f"[Consumer] Failed: {response.text}")

@utility.timer
def test_function():
    # 1. 配置各个 HTTP Trigger 的 URL
    # 这里的地址假设是本地调试使用的 URL，也可能是 Azure Functions 提供的公网地址
    # 例如 "http://<你的函数地址>/api/http_trigger_manage_topic"
    manage_topic_url = "https://producerconsumer2.azurewebsites.net/api/http_trigger_manage_topic?code=Z-zjJnCr6XPE1WTChnR4ybX8x-Xnk4XmYAlyF2_2fXTrAzFumz_7xQ=="
    producer_url = "https://producerconsumer2.azurewebsites.net/api/http_trigger_producer?code=fy7ecbTu3OvSiVmKCoy2pc6gnPCHH7sRjqVVJoNikIUuAzFu2e6_jQ=="
    consumer_url = "https://producerconsumer2.azurewebsites.net/api/http_trigger_consumer?code=K4ebUBWMqstk8To_1Unoi070HzfDEJvgn5pM5nIALjQ3AzFuCfbTXQ=="

    # 2. 请求体（Payload）配置
    num_messages = 2000
    manage_topic_payload = {
        "broker_address": "vmforkafka.southcentralus.cloudapp.azure.com:9092",
        "topic_name": "test-throughput",
        "num_partitions": 3,
        "replication_factor": 1
    }

    producer_payload = {
        "broker_address": "vmforkafka.southcentralus.cloudapp.azure.com:9092",
        "topic": "test-throughput",
        "num_messages": num_messages,
        "batch_size": 16384,
        "linger_ms": 5,
        "compression_type": "lz4"
    }

    consumer_payload = {
        "broker_address": "vmforkafka.southcentralus.cloudapp.azure.com:9092",
        "topic": "test-throughput",
        "num_messages": num_messages,
        "group_id": "latency-test-group"
    }

    # 3. 先删除并重新创建 Topic
    print("======== Step 1: Manage topic ========")
    call_manage_topic(manage_topic_url, manage_topic_payload)

    # 4. 使用线程池同时启动多个 Producer 和 Consumer
    print("======== Step 2: Start Multiple Producers & Consumers with ThreadPoolExecutor ========")

    num_producers = 3  # 你可以根据需要调整这个数量
    num_consumers = 3  # 你可以根据需要调整这个数量

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 提交所有 Producer 任务
        producer_futures = [
            executor.submit(call_producer, producer_url, producer_payload)
            for _ in range(num_producers)
        ]

        # 提交所有 Consumer 任务
        consumer_futures = [
            executor.submit(call_consumer, consumer_url, consumer_payload)
            for _ in range(num_consumers)
        ]

        # 等待所有 Producer 任务完成
        for future in concurrent.futures.as_completed(producer_futures):
            future.result()  # 这里可以处理返回值或异常

        # 等待所有 Consumer 任务完成
        for future in concurrent.futures.as_completed(consumer_futures):
            future.result()  # 这里可以处理返回值或异常

    print("======== Test Completed ========")


if __name__ == "__main__":
    test_function()
