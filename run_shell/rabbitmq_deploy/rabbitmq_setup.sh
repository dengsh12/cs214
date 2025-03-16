#!/bin/bash

# 设置环境变量
ERLANG_COOKIE="RABBITMQ_CLUSTER_COOKIE_VALUE"
RABBITMQ_NODES=( "rabbitmq-1;10.128.0.12;34.59.119.117" "rabbitmq-2;10.128.0.10;34.68.113.17" "rabbitmq-3;10.128.0.11;35.238.173.156" )
HOSTNAME=$(hostname)

# 获取当前节点的IP
CURRENT_NODE_IP=""
CURRENT_NODE_NAME=""
for node in "${RABBITMQ_NODES[@]}"; do
  node_name=$(echo $node | cut -d';' -f1)
  node_internal_ip=$(echo $node | cut -d';' -f2)
  
  if [[ "$HOSTNAME" == "$node_name" ]]; then
    CURRENT_NODE_IP=$node_internal_ip
    CURRENT_NODE_NAME=$node_name
    break
  fi
done

if [[ -z "$CURRENT_NODE_IP" ]]; then
  echo "当前主机名 $HOSTNAME 不在RabbitMQ节点列表中"
  exit 1
fi

echo "当前节点: $CURRENT_NODE_NAME, IP: $CURRENT_NODE_IP"

# 停止并移除已存在的容器
echo "停止并移除已存在的RabbitMQ容器..."
sudo docker stop rabbitmq 2>/dev/null || true
sudo docker rm rabbitmq 2>/dev/null || true

# 检查并终止已运行的epmd进程
echo "检查并终止已运行的epmd进程..."
sudo pkill -f epmd || true
sleep 2

# 安装lsof（如果需要）
if ! command -v lsof &> /dev/null; then
    echo "安装lsof..."
    sudo apt-get update -y
    sudo apt-get install -y lsof
fi

# 检查端口占用情况
echo "检查端口占用情况..."
sudo lsof -i:4369 || true
sudo lsof -i:5672 || true
sudo lsof -i:15672 || true
sudo lsof -i:25672 || true

# 拉取RabbitMQ镜像
echo "拉取RabbitMQ镜像..."
sudo docker pull rabbitmq:3.9-management

# 创建Erlang Cookie文件
echo "创建Erlang Cookie文件..."
mkdir -p ~/.erlang.cookie/
echo -n "$ERLANG_COOKIE" > ~/.erlang.cookie/cookie
chmod 400 ~/.erlang.cookie/cookie

# 启动RabbitMQ容器
echo "启动RabbitMQ容器..."
sudo docker run -d --name rabbitmq \
  --hostname $CURRENT_NODE_NAME \
  -p 5672:5672 \
  -p 15672:15672 \
  -p 4369:4369 \
  -p 25672:25672 \
  --net=host \
  -e RABBITMQ_ERLANG_COOKIE=$ERLANG_COOKIE \
  -v ~/.erlang.cookie/cookie:/var/lib/rabbitmq/.erlang.cookie \
  --add-host rabbitmq-1:10.128.0.12 \
  --add-host rabbitmq-2:10.128.0.10 \
  --add-host rabbitmq-3:10.128.0.11 \
  rabbitmq:3.9-management

# 等待RabbitMQ启动
echo "等待RabbitMQ启动..."
sleep 30

# 检查容器是否成功运行
echo "检查容器状态..."
CONTAINER_RUNNING=$(sudo docker ps -q -f name=rabbitmq)
if [[ -z "$CONTAINER_RUNNING" ]]; then
  echo "容器启动失败，检查日志..."
  sudo docker logs rabbitmq
  exit 1
fi

# 启用管理插件
echo "启用管理插件..."
sudo docker exec rabbitmq rabbitmq-plugins enable rabbitmq_management

# 如果这是第一个节点，不需要加入集群
if [[ "$CURRENT_NODE_NAME" == "rabbitmq-1" ]]; then
  echo "这是集群的第一个节点，不需要加入集群"
else
  # 检查第一个节点是否可达
  echo "检查rabbitmq-1节点是否可达..."
  if ping -c 2 rabbitmq-1 &> /dev/null; then
    echo "将节点加入集群..."
    sudo docker exec rabbitmq rabbitmqctl stop_app
    sudo docker exec rabbitmq rabbitmqctl reset
    
    # 尝试加入集群并确保应用程序启动
    if sudo docker exec rabbitmq rabbitmqctl join_cluster rabbit@rabbitmq-1; then
      echo "成功加入集群，现在启动应用程序..."
      sudo docker exec rabbitmq rabbitmqctl start_app
    else
      echo "加入集群失败，尝试独立运行..."
      sudo docker exec rabbitmq rabbitmqctl start_app
    fi
  else
    echo "无法连接到rabbitmq-1，继续作为独立节点运行"
    sudo docker exec rabbitmq rabbitmqctl start_app
  fi
fi

# 检查集群状态
echo "检查集群状态..."
sudo docker exec rabbitmq rabbitmqctl cluster_status

echo "$CURRENT_NODE_NAME 设置完成！"