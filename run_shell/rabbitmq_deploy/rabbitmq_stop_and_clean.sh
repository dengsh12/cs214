#!/bin/bash

# RabbitMQ节点信息
RABBITMQ_NODES=( "rabbitmq-1;10.128.0.12;34.59.119.117" "rabbitmq-2;10.128.0.10;34.68.113.17" "rabbitmq-3;10.128.0.11;35.238.173.156" )
ZONE="us-central1-c"

echo "开始停止和清理RabbitMQ集群..."

# 在所有节点上停止和清理RabbitMQ
for node in "${RABBITMQ_NODES[@]}"; do
  node_name=$(echo $node | cut -d';' -f1)
  node_external_ip=$(echo $node | cut -d';' -f3)
  
  echo "正在清理节点: $node_name ($node_external_ip)..."
  
  # 停止并移除RabbitMQ容器
  gcloud compute ssh --zone=$ZONE $node_name -- "sudo docker stop rabbitmq 2>/dev/null || true && sudo docker rm rabbitmq 2>/dev/null || true"
  
  # 终止epmd进程
  gcloud compute ssh --zone=$ZONE $node_name -- "sudo pkill -f epmd || true"
  
  # 清理erlang cookie
  gcloud compute ssh --zone=$ZONE $node_name -- "rm -f ~/.erlang.cookie/cookie && rm -rf ~/.erlang.cookie"
  
  # 清理脚本
  gcloud compute ssh --zone=$ZONE $node_name -- "rm -f ~/rabbitmq_setup.sh"
done

echo "验证清理是否完成..."
for node in "${RABBITMQ_NODES[@]}"; do
  node_name=$(echo $node | cut -d';' -f1)
  
  echo "检查节点 $node_name 上的容器状态:"
  gcloud compute ssh --zone=$ZONE $node_name -- "sudo docker ps -a | grep rabbitmq || echo '没有找到RabbitMQ容器'"
done

echo "RabbitMQ集群清理完成！"