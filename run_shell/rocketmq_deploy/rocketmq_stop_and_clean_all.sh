#!/usr/bin/env bash
#
# rocketmq_stop_and_clean_all.sh
# 用途：在 GCP 三台 RocketMQ 节点上停止进程，并彻底清理数据目录。
# -------------------------------------------------------------------
# 1. 停止 NameServer 和 Broker 的进程（若存在）
# 2. 删除本地的 ~/rocketmq-data 目录以及相关日志文件
# 注意：此操作会删除所有数据，所有旧的 topic 和消息都会消失，请谨慎使用
#
# 使用方法:
#   chmod +x rocketmq_stop_and_clean_all.sh
#   ./rocketmq_stop_and_clean_all.sh

# =============== 配置区域 ===============
INSTANCES=("mq-1" "mq-2" "mq-3")
ZONE="us-central1-c"  
# 如果三台实例不在同一 zone，请修改为相应配置

# =============== 脚本开始 ===============
for INSTANCE in "${INSTANCES[@]}"; do
  echo ">>> 正在检查实例: $INSTANCE"

  # 获取实例当前状态（如 RUNNING, TERMINATED, STOPPED 等）
  STATUS=$(gcloud compute instances describe "$INSTANCE" \
           --zone="$ZONE" \
           --format="value(status)")

  if [ "$STATUS" == "RUNNING" ]; then
    echo "实例 $INSTANCE 处于 RUNNING 状态，准备停止并清理..."

    gcloud compute ssh "$INSTANCE" --zone="$ZONE" --command="bash -c '
      echo \"--- 1) 停止 RocketMQ NameServer 和 Broker ---\";
      # 通过 lsof 根据监听端口查找进程 PID
      ns_pid=\$(lsof -ti :9876);
      if [ -n \"\$ns_pid\" ]; then
          echo \"检测到 NameServer 进程 PID: \$ns_pid，开始 kill\";
          kill -9 \$ns_pid;
      else
          echo \"未检测到 NameServer 进程。\";
      fi
      broker_pid=\$(lsof -ti :10911);
      if [ -n \"\$broker_pid\" ]; then
          echo \"检测到 Broker 进程 PID: \$broker_pid，开始 kill\";
          kill -9 \$broker_pid;
      else
          echo \"未检测到 Broker 进程。\";
      fi

      echo \"--- 2) 删除数据目录 ~/rocketmq-data 及日志文件 ---\";
      rm -rf \$HOME/rocketmq-data || true;
      rm -f \$HOME/namesrv.log || true;
      rm -f \$HOME/rocketmq_broker.log || true;
      echo \"数据目录及日志已清理完毕。\";
      echo \"完成。\"
      # 停止 NameServer (如果存在)
      if [ -f bin/mqshutdown ]; then
        sh bin/mqshutdown namesrv;
      fi
    '"

    echo "[$INSTANCE] 已执行停止+清理操作。"
  else
    echo "实例 $INSTANCE 当前状态：$STATUS，无需处理。"
  fi

  echo "======================================================"
done

echo "全部操作完成。"
