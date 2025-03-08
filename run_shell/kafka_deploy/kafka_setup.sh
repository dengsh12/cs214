#!/bin/bash
# kafka_setup.sh
# 用途：在单台机器上安装 Java、下载 Kafka、配置并启动 ZooKeeper 和 Kafka Broker。
#
# 使用方法（在每台 VM 上分别运行）：
#   sudo bash kafka_setup.sh <NODE_ID>
#   NODE_ID: 集群中该节点的编号（1, 2 或 3）
#
# 注意：
# - 该脚本区分内部（私网）和外部（公网）的 IP：
#     - ZooKeeper 间的通信使用私网 IP（例如 10.128.0.x）。
#     - Kafka 对外暴露的 advertised.listeners 使用公网 IP（例如 35.x.x.x）。
#
# 请根据你的环境修改以下 IP 地址变量：

# 私网 IP（内部通信使用）
PRIVATE_NODE1_IP="10.128.0.3"
PRIVATE_NODE2_IP="10.128.0.4"
PRIVATE_NODE3_IP="10.128.0.5"

# 公网 IP（外部客户端连接使用）
PUBLIC_NODE1_IP="35.209.251.221"
PUBLIC_NODE2_IP="35.239.56.104"
PUBLIC_NODE3_IP="35.208.205.25"

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <NODE_ID>"
    exit 1
fi

NODE_ID=$1

# 根据节点编号确定本机私网和公网 IP
if [ "$NODE_ID" -eq 1 ]; then
    THIS_PRIVATE_IP=$PRIVATE_NODE1_IP
    THIS_PUBLIC_IP=$PUBLIC_NODE1_IP
elif [ "$NODE_ID" -eq 2 ]; then
    THIS_PRIVATE_IP=$PRIVATE_NODE2_IP
    THIS_PUBLIC_IP=$PUBLIC_NODE2_IP
elif [ "$NODE_ID" -eq 3 ]; then
    THIS_PRIVATE_IP=$PRIVATE_NODE3_IP
    THIS_PUBLIC_IP=$PUBLIC_NODE3_IP
else
    echo "Invalid NODE_ID. Must be 1, 2, or 3."
    exit 1
fi

# ================================
# Kafka 相关参数 —— 使用 Kafka 3.5.1 稳定版本
KAFKA_DIR="$HOME/kafka"
KAFKA_VERSION="3.5.1"
SCALA_VERSION="2.13"
KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
# 下载地址：使用 Apache 归档服务器
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"

# ================================
# 安装 Java（需要 sudo 权限）
if ! command -v java &> /dev/null; then
    echo "Java not found, installing OpenJDK 17..."
    sudo apt-get update
    sudo apt-get install -y openjdk-17-jdk
fi

# ================================
# 下载并解压 Kafka（放在用户目录下，不需要 sudo）
if [ ! -d "$KAFKA_DIR" ]; then
    echo "Downloading Kafka from $KAFKA_URL ..."
    wget $KAFKA_URL -O /tmp/$KAFKA_TGZ
    if [ $? -ne 0 ]; then
        echo "Failed to download Kafka from $KAFKA_URL"
        exit 1
    fi
    tar -xzf /tmp/$KAFKA_TGZ -C $HOME
    mv "$HOME/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "$KAFKA_DIR"
fi

# ================================
# 配置 ZooKeeper
ZK_CONFIG="$KAFKA_DIR/config/zookeeper.properties"
sed -i 's|^dataDir=.*|dataDir='"$HOME"'/zookeeper_data|g' $ZK_CONFIG

cat > $KAFKA_DIR/config/zoo.cfg <<EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$HOME/zookeeper_data
clientPort=2181
clientPortAddress=0.0.0.0
server.1=${PRIVATE_NODE1_IP}:2888:3888
server.2=${PRIVATE_NODE2_IP}:2888:3888
server.3=${PRIVATE_NODE3_IP}:2888:3888
EOF

mkdir -p $HOME/zookeeper_data
echo $NODE_ID > $HOME/zookeeper_data/myid

# ================================
# 配置 Kafka Broker
KAFKA_CONFIG="$KAFKA_DIR/config/server.properties"
sed -i "s|^broker.id=.*|broker.id=$NODE_ID|g" $KAFKA_CONFIG
sed -i 's|^log.dirs=.*|log.dirs='"$HOME"'/kafka-logs|g' $KAFKA_CONFIG
sed -i "s|^#listeners=PLAINTEXT://:9092|listeners=PLAINTEXT://0.0.0.0:9092|g" $KAFKA_CONFIG
# 替换所有 advertised.listeners 行，确保使用正确的公网 IP
sed -i "s|^#*advertised.listeners=.*|advertised.listeners=PLAINTEXT://$THIS_PUBLIC_IP:9092|g" $KAFKA_CONFIG
# 设置 ZooKeeper 连接字符串，使用私网 IP
ZK_CONNECT="${PRIVATE_NODE1_IP}:2181,${PRIVATE_NODE2_IP}:2181,${PRIVATE_NODE3_IP}:2181"
sed -i "s|^zookeeper.connect=.*|zookeeper.connect=${ZK_CONNECT}|g" $KAFKA_CONFIG

# ===== 在 server.properties 里增加并发配置，以匹配 RocketMQ
cat >> $KAFKA_CONFIG <<EOF

#####################
# For Fair Comparison
#####################
num.network.threads=2
num.io.threads=2
EOF

# ================================
# 启动 ZooKeeper 和 Kafka Broker
echo "Starting ZooKeeper..."
nohup $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zoo.cfg > $HOME/zookeeper.log 2>&1 &
sleep 5
echo "Starting Kafka broker..."
nohup $KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_CONFIG > $HOME/kafka.log 2>&1 &
echo "Kafka node $NODE_ID started."
echo "Private IP (for internal ZooKeeper): $THIS_PRIVATE_IP"
echo "Public IP (for external Kafka clients): $THIS_PUBLIC_IP"
echo "ZooKeeper log: $HOME/zookeeper.log"
echo "Kafka log: $HOME/kafka.log"
