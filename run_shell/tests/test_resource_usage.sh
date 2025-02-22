#!/bin/bash
# test_resources.sh
# 运行测试并观察资源使用情况
python ./main.py \
  --mq_type kafka \
  --broker_address 35.209.251.221 \
  --topic test-throughput_resources \
  --num_producers 2 \
  --num_consumers 2 \
  --messages_per_producer 100 \
  --log_interval 100 \
  --remote_ips "35.209.251.221,35.239.56.104,35.208.205.25"