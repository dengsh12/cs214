#!/bin/bash
# rocketmq_setup.sh
# 在单台机器上安装 Java、下载 RocketMQ、配置并启动 NameServer 和 Broker，
# 并在 broker.conf 中设置异步刷盘与适当线程池，确保对比更公平。
#
# 用法（在每台 VM 上分别运行）:
#   sudo ./rocketmq_setup.sh <NODE_ID>
#   NODE_ID: 集群节点编号（1, 2, 3）
#
# 自行修改以下 IP 变量以匹配环境:

# 私网 IP（内部通信使用）
PRIVATE_NODE1_IP="10.128.0.3"
PRIVATE_NODE2_IP="10.128.0.4"
PRIVATE_NODE3_IP="10.128.0.5"

# 公网 IP（外部连接使用）
PUBLIC_NODE1_IP="35.209.251.221"
PUBLIC_NODE2_IP="35.239.56.104"
PUBLIC_NODE3_IP="35.208.205.25"

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <NODE_ID>"
    exit 1
fi

NODE_ID=$1

# 根据节点编号决定私网/公网 IP
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
# RocketMQ 相关
ROCKETMQ_DIR="$HOME/rocketmq"
ROCKETMQ_VERSION="4.9.8"
ROCKETMQ_TGZ="rocketmq-all-${ROCKETMQ_VERSION}-bin-release.zip"
ROCKETMQ_URL="https://archive.apache.org/dist/rocketmq/${ROCKETMQ_VERSION}/${ROCKETMQ_TGZ}"

# 安装 Java
if ! command -v java &> /dev/null; then
    echo "Java not found, installing OpenJDK 17..."
    sudo apt-get update
    sudo apt-get install -y openjdk-17-jdk
fi

# 安装 unzip
if ! command -v unzip &> /dev/null; then
    echo "unzip not found, installing unzip..."
    sudo apt-get update
    sudo apt-get install -y unzip
fi

# 下载 & 解压
if [ ! -d "$ROCKETMQ_DIR" ]; then
    echo "Downloading RocketMQ from $ROCKETMQ_URL"
    wget $ROCKETMQ_URL -O /tmp/$ROCKETMQ_TGZ
    if [ $? -ne 0 ]; then
        echo "Failed to download RocketMQ"
        exit 1
    fi
    unzip /tmp/$ROCKETMQ_TGZ -d $HOME
    mv "$HOME/rocketmq-all-${ROCKETMQ_VERSION}-bin-release" "$ROCKETMQ_DIR"
fi

# 配置 broker.conf
BROKER_CONFIG="$ROCKETMQ_DIR/conf/broker.conf"
mkdir -p "$ROCKETMQ_DIR/conf"

cat > $BROKER_CONFIG <<EOF
namesrvAddr=${PRIVATE_NODE1_IP}:9876;${PRIVATE_NODE2_IP}:9876;${PRIVATE_NODE3_IP}:9876
brokerClusterName=DefaultCluster
brokerName=broker-$NODE_ID
brokerId=0
brokerIP1=${THIS_PUBLIC_IP}
listenPort=10911
storePathRootDir=\$HOME/rocketmq-data
storePathCommitLog=\$HOME/rocketmq-data/commitlog

# 使用异步刷盘
flushDiskType=ASYNC_FLUSH

# 适当大小的线程池(2核机器用128过大，改为16更合理)
sendMessageThreadPoolNums=2
putMessageThreadPoolNums=2
EOF

mkdir -p $HOME/rocketmq-data

# JDK17 反射访问参数
EXTRA_OPENS="--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"

export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS $EXTRA_OPENS"

echo "Starting RocketMQ NameServer..."
nohup $ROCKETMQ_DIR/bin/mqnamesrv > $HOME/namesrv.log 2>&1 &
sleep 5

echo "Starting RocketMQ Broker..."
nohup $ROCKETMQ_DIR/bin/mqbroker \
  -n "${PRIVATE_NODE1_IP}:9876;${PRIVATE_NODE2_IP}:9876;${PRIVATE_NODE3_IP}:9876" \
  -c $BROKER_CONFIG \
  > $HOME/rocketmq_broker.log 2>&1 &

echo "RocketMQ node $NODE_ID started."
echo "Private IP: $THIS_PRIVATE_IP"
echo "Public IP:  $THIS_PUBLIC_IP"
echo "NameServer log:  $HOME/namesrv.log"
echo "Broker log:      $HOME/rocketmq_broker.log"
