#!/bin/bash
# test_resources.sh
# 运行测试并观察资源使用情况
python ./main.py \
  --mq_type kafka \
  --broker_address 35.209.251.221 \
  --topic test-throughput_resources \
  --num_producers 2 \
  --num_consumers 2 \
  --messages_per_producer 500 \
  --log_interval 100
