#!/bin/bash
# test_scaling.sh
# 依次测试不同消费者数量下的性能指标
for consumers in 10 20 30 40 50; do
    echo "----- 测试消费者数量: $consumers -----"
    python main.py \
      --mq_type kafka \
      --broker_address 35.209.251.221 \
      --topic test-throughput_scaling \
      --num_producers 50 \
      --num_consumers $consumers \
      --messages_per_producer 1000 \
      --log_interval 100
      --remote_ips "35.209.251.221,35.239.56.104,35.208.205.25"
    sleep 10
done
