#!/bin/bash
# test_scaling_rocketmq.sh
# 分别测试不同消费者数量和不同消息数量下的 RocketMQ 性能指标

for consumers in 5 10 20 30 40; do
    for messages in 50000 1000000 5000000; do
        echo "----- 测试消费者数量: $consumers, 每个生产者消息数: $messages -----"
        python main.py \
            --mq_type rocketmq \
            --namesrv_addr "35.209.251.221:9876;35.239.56.104:9876;35.208.205.25:9876" \
            --mqadmin_path "/home/songh00/rocketmq_temp/rocketmq-all-4.9.8-bin-release/bin/mqadmin" \
            --topic test-throughput_scaling_rocketmq \
            --num_producers 40 \
            --num_consumers $consumers \
            --messages_per_producer $messages \
            --log_interval 100 \
            --remote_ips "35.209.251.221,35.239.56.104,35.208.205.25"
        sleep 10
    done
done
