#!/bin/bash

# RabbitMQ节点信息
RABBITMQ_NODES=( "rabbitmq-1;10.128.0.12;34.59.119.117" "rabbitmq-2;10.128.0.10;34.68.113.17" "rabbitmq-3;10.128.0.11;35.238.173.156" )
ZONE="us-central1-c"

echo "开始部署RabbitMQ集群..."

# 首先清理所有节点
echo "清理所有节点上的旧部署..."
for node in "${RABBITMQ_NODES[@]}"; do
  node_name=$(echo $node | cut -d';' -f1)
  node_external_ip=$(echo $node | cut -d';' -f3)
  
  echo "正在清理节点: $node_name ($node_external_ip)..."
  gcloud compute ssh --zone=$ZONE $node_name -- "sudo docker stop rabbitmq 2>/dev/null || true && sudo docker rm rabbitmq 2>/dev/null || true && sudo pkill -f epmd || true"
done

# 等待所有节点清理完成
echo "等待清理完成..."
sleep 10

# 按顺序部署节点，确保第一个节点先完成部署
for node in "${RABBITMQ_NODES[@]}"; do
  node_name=$(echo $node | cut -d';' -f1)
  node_external_ip=$(echo $node | cut -d';' -f3)
  
  echo "正在部署节点: $node_name ($node_external_ip)..."
  
  # 复制设置脚本到目标节点
  echo "复制设置脚本到 $node_name..."
  gcloud compute scp --zone=$ZONE rabbitmq_setup.sh $node_name:~/rabbitmq_setup.sh
  
  # 设置脚本执行权限并执行
  echo "执行设置脚本在 $node_name..."
  gcloud compute ssh --zone=$ZONE $node_name -- "chmod +x ~/rabbitmq_setup.sh && ~/rabbitmq_setup.sh"
  
  # 如果是第一个节点，多等待一些时间确保它启动完成
  if [[ "$node_name" == "rabbitmq-1" ]]; then
    echo "第一个节点部署完成，等待60秒确保其完全启动..."
    sleep 60
    
    # 确认第一个节点已正常启动
    node_status=$(gcloud compute ssh --zone=$ZONE $node_name -- "sudo docker ps -q -f name=rabbitmq | wc -l")
    if [[ "$node_status" -eq "0" ]]; then
      echo "警告：第一个节点可能未正常启动，集群可能无法正确形成"
    else
      echo "第一个节点已成功启动"
    fi
  else
    # 等待其他节点完成
    sleep 10
  fi
done

# 验证集群配置
echo "等待集群稳定..."
sleep 30

# 确保所有节点应用程序都已启动
echo "确保所有节点的应用程序都已启动..."
for node in "${RABBITMQ_NODES[@]}"; do
  node_name=$(echo $node | cut -d';' -f1)
  node_external_ip=$(echo $node | cut -d';' -f3)
  
  echo "检查节点 $node_name 的应用程序状态..."
  app_status=$(gcloud compute ssh --zone=$ZONE $node_name -- "sudo docker exec rabbitmq rabbitmqctl status | grep rabbit 2>/dev/null || echo '应用未运行'")
  
  if [[ "$app_status" == *"应用未运行"* ]]; then
    echo "节点 $node_name 的应用程序未运行，尝试启动..."
    gcloud compute ssh --zone=$ZONE $node_name -- "sudo docker exec rabbitmq rabbitmqctl start_app || true"
  else
    echo "节点 $node_name 的应用程序正在运行"
  fi
done

# 等待额外的30秒，让应用程序完全启动
sleep 30

# 验证集群状态
echo "验证RabbitMQ集群状态..."
for node in "${RABBITMQ_NODES[@]}"; do
  node_name=$(echo $node | cut -d';' -f1)
  node_external_ip=$(echo $node | cut -d';' -f3)
  
  echo "检查节点 $node_name 的集群状态:"
  gcloud compute ssh --zone=$ZONE $node_name -- "sudo docker exec rabbitmq rabbitmqctl cluster_status"
done

echo "RabbitMQ集群部署完成！"

# 显示集群节点总数
echo "集群形成情况检查:"
gcloud compute ssh --zone=$ZONE rabbitmq-1 -- "sudo docker exec rabbitmq rabbitmqctl cluster_status | grep -A 10 'Running Nodes' | grep rabbit@ | wc -l"

# 打印集群连接信息
echo -e "\n集群连接信息:"
echo "主节点: rabbitmq-1 (内部IP: 10.128.0.12, 外部IP: 34.59.119.117)"
echo "管理界面: http://34.59.119.117:15672"
echo "用户名/密码: guest/guest"