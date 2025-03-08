#!/bin/bash
# rocketmq_setup.sh
# 用途：在单台机器上安装 Java、下载 RocketMQ、配置并启动 NameServer 和 Broker。
#
# 使用方法（在每台 VM 上分别运行）：
#   sudo ./rocketmq_setup.sh <NODE_ID>
#   NODE_ID: 集群中该节点的编号（1, 2 或 3）
#
# 注意：
# - apt-get 部分需要 sudo 权限，RocketMQ 部分运行在用户目录下 ($HOME)。
# - 此脚本区分内部（私网）和外部（公网）IP：
#     - NameServer 通信使用私网 IP（例如 10.128.0.x）。
#     - Broker 对外公布时使用公网 IP（例如 35.x.x.x）。
#
# 请根据你环境实际情况修改下面的 IP 地址变量。

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
# RocketMQ 相关参数
ROCKETMQ_DIR="$HOME/rocketmq"
ROCKETMQ_VERSION="4.9.8"
ROCKETMQ_TGZ="rocketmq-all-${ROCKETMQ_VERSION}-bin-release.zip"
ROCKETMQ_URL="https://archive.apache.org/dist/rocketmq/${ROCKETMQ_VERSION}/${ROCKETMQ_TGZ}"

# ================================
# 安装 Java（需要 sudo 权限）
if ! command -v java &> /dev/null; then
    echo "Java not found, installing OpenJDK 17..."
    sudo apt-get update
    sudo apt-get install -y openjdk-17-jdk
fi

# 检查 unzip 是否存在，否则安装 unzip
if ! command -v unzip &> /dev/null; then
    echo "unzip not found, installing unzip..."
    sudo apt-get update
    sudo apt-get install -y unzip
fi

# ================================
# 下载并解压 RocketMQ（放在用户目录下，不需要 sudo）
if [ ! -d "$ROCKETMQ_DIR" ]; then
    echo "Downloading RocketMQ from $ROCKETMQ_URL ..."
    wget $ROCKETMQ_URL -O /tmp/$ROCKETMQ_TGZ
    if [ $? -ne 0 ]; then
        echo "Failed to download RocketMQ from $ROCKETMQ_URL"
        exit 1
    fi
    unzip /tmp/$ROCKETMQ_TGZ -d $HOME
    if [ -d "$HOME/rocketmq-all-${ROCKETMQ_VERSION}-bin-release" ]; then
        mv "$HOME/rocketmq-all-${ROCKETMQ_VERSION}-bin-release" "$ROCKETMQ_DIR"
    else
        echo "解压目录不存在，请检查压缩包内容。"
        exit 1
    fi
fi

# ================================
# 配置 RocketMQ Broker
BROKER_CONFIG="$ROCKETMQ_DIR/conf/broker.conf"
mkdir -p "$ROCKETMQ_DIR/conf"
cat > $BROKER_CONFIG <<EOF
# RocketMQ Broker 配置文件

# 指定 NameServer 地址，使用私网 IP 进行内部通信
namesrvAddr=${PRIVATE_NODE1_IP}:9876;${PRIVATE_NODE2_IP}:9876;${PRIVATE_NODE3_IP}:9876

# Broker 的集群名称
brokerClusterName=DefaultCluster

# Broker 的名称（每个节点唯一）
brokerName=broker-$NODE_ID

# 固定为 0，表示 Master 节点
brokerId=0

# Broker 对外公布的 IP 地址（公网）
brokerIP1=${THIS_PUBLIC_IP}

# Broker 监听端口
listenPort=10911

# 存储路径配置
storePathRootDir=\$HOME/rocketmq-data
storePathCommitLog=\$HOME/rocketmq-data/commitlog
EOF

# 创建 RocketMQ 数据目录
mkdir -p $HOME/rocketmq-data

# ================================
# 为 JDK 17 下的反射访问问题添加必要参数
# 不仅要加 --add-opens，若访问 jdk.internal.ref.Cleaner 则还要 --add-exports
EXTRA_OPENS="--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"

# 把这些参数加到 JAVA_TOOL_OPTIONS，从而保证 mqnamesrv、mqbroker 都带上
export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS $EXTRA_OPENS"

# ================================
# 启动 NameServer
echo "Starting RocketMQ NameServer..."
nohup $ROCKETMQ_DIR/bin/mqnamesrv > $HOME/namesrv.log 2>&1 &
sleep 5

# 启动 Broker
echo "Starting RocketMQ Broker..."
nohup $ROCKETMQ_DIR/bin/mqbroker \
  -n "${PRIVATE_NODE1_IP}:9876;${PRIVATE_NODE2_IP}:9876;${PRIVATE_NODE3_IP}:9876" \
  -c $BROKER_CONFIG \
  > $HOME/rocketmq_broker.log 2>&1 &

echo "RocketMQ node $NODE_ID started."
echo "Private IP (for internal communication): $THIS_PRIVATE_IP"
echo "Public IP (for external clients): $THIS_PUBLIC_IP"
echo "NameServer log: $HOME/namesrv.log"
echo "Broker log: $HOME/rocketmq_broker.log"
