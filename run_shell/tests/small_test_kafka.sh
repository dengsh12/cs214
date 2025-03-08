#!/bin/bash
# test_resources.sh
# 运行测试并观察资源使用情况，同时指定消息大小

python ./main.py \
  --mq_type kafka \
  --broker_address 35.209.251.221 \
  --topic test-throughput_resources \
  --num_producers 5 \
  --num_consumers 5 \
  --messages_per_producer 450 \
  --log_interval 100 \
  --message_size 100 \
  --remote_ips "35.209.251.221,35.239.56.104,35.208.205.25"
