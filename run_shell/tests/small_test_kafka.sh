#!/bin/bash
# test_resources.sh
# 运行测试并观察资源使用情况，同时指定消息大小
bash ./run_shell/kafka_deploy/stop_kafka.sh
bash ./run_shell/kafka_deploy/start_kafka.sh
python ./main.py \
  --mq_type kafka \
  --broker_address "35.209.251.221:9092,35.239.56.104:9092,35.208.205.25:9092" \
  --topic test-throughput_resources \
  --num_producers 10 \
  --num_consumers 5 \
  --messages_per_producer 5000 \
  --log_interval 1000 \
  --message_size 100 \
  --remote_ips "35.209.251.221,35.239.56.104,35.208.205.25"
bash ./run_shell/kafka_deploy/stop_kafka.sh