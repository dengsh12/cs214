#!/usr/bin/env bash
#
# stop_and_clean_all.sh
# 用途：一键在 GCP 三台 Kafka 节点上停止进程，并彻底清理数据目录。
# -------------------------------------------------------------------
# 1. 停止 ZooKeeper 和 Kafka 的进程（若存在）
# 2. 删除本地的 ~/zookeeper_data 和 ~/kafka-logs
# 3. 因为会删除数据，所有旧的 topic 和消息都会消失，谨慎使用
#
# 使用方法:
#   chmod +x stop_and_clean_all.sh
#   ./stop_and_clean_all.sh

# =============== 配置区域 ===============
INSTANCES=("kafka-1" "kafka-2" "kafka-3")
ZONE="us-central1-c"  
# 如果三台不在同一个 zone，需要改成数组或分别指定

# =============== 脚本开始 ===============
for INSTANCE in "${INSTANCES[@]}"; do
  echo ">>> 正在检查实例: $INSTANCE"

  # 获取实例当前状态（如 RUNNING, TERMINATED, STOPPED 等）
  STATUS=$(gcloud compute instances describe "$INSTANCE" \
           --zone="$ZONE" \
           --format="value(status)")

  if [ "$STATUS" == "RUNNING" ]; then
    echo "实例 $INSTANCE 处于 RUNNING 状态，准备停止并清理..."

    # 下面的命令会在远程主机上执行：
    #   1. 调用 Kafka 自带的 stop 脚本（若存在）；否则 kill 进程
    #   2. 删除 zookeeper_data、kafka-logs 目录
    gcloud compute ssh "$INSTANCE" --zone="$ZONE" --command="\
      echo '--- 1) 停止 ZK/Kafka 进程 (若脚本存在则用脚本，否则 kill) ---';
      if [ -x '\$HOME/kafka/bin/zookeeper-server-stop.sh' ]; then
        \$HOME/kafka/bin/zookeeper-server-stop.sh || true
      fi
      if [ -x '\$HOME/kafka/bin/kafka-server-stop.sh' ]; then
        \$HOME/kafka/bin/kafka-server-stop.sh || true
      fi

      echo '--- 若进程仍在，则 kill ---';
      zkpid=\$(ps -ef | grep 'org.apache.zookeeper.server.quorum.QuorumPeerMain' | grep -v grep | awk '{print \$2}')
      if [ ! -z \"\$zkpid\" ]; then
        kill -9 \$zkpid || true
        echo '已 kill ZooKeeper 进程(\$zkpid)'
      fi
      kafkaPid=\$(ps -ef | grep 'kafka.Kafka' | grep -v grep | awk '{print \$2}')
      if [ ! -z \"\$kafkaPid\" ]; then
        kill -9 \$kafkaPid || true
        echo '已 kill Kafka 进程(\$kafkaPid)'
      fi

      echo '--- 2) 删除数据目录 ~/zookeeper_data & ~/kafka-logs ---';
      rm -rf \$HOME/zookeeper_data || true
      rm -rf \$HOME/kafka-logs   || true
      echo '数据目录已清理完毕。'

      echo '完成。'
    "

    echo "[$INSTANCE] 已执行停止+清理操作。"
  else
    echo "实例 $INSTANCE 当前状态：$STATUS，无需处理。"
  fi

  echo "======================================================"
done

echo "全部操作完成。"
