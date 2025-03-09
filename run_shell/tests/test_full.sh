#!/bin/bash
# test_scaling.sh
# 依次测试不同消费者数量和不同消息数量下的性能指标

for consumers in 5 10 20 40; do
    for messages in 50000 2000000; do
        echo "----- 测试消费者数量: $consumers, 每个生产者消息数: $messages -----"
        python main.py \
            --mq_type kafka \
            --broker_address 35.209.251.221 \
            --topic test-throughput_scaling \
            --num_producers 40 \
            --num_consumers $consumers \
            --messages_per_producer $messages \
            --log_interval 100 \
            --remote_ips "35.209.251.221,35.239.56.104,35.208.205.25"
        sleep 10
    done
done
