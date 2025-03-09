#!/usr/bin/env bash
#
# stop_all_instances.sh
# 一键关闭 (stop) GCP 上的三台 Kafka 虚机
#

INSTANCES=("mq-1" "mq-2" "mq-3")
ZONE="us-central1-c"

for INSTANCE in "${INSTANCES[@]}"; do
  echo ">>> 正在检查实例: $INSTANCE"
  STATUS=$(gcloud compute instances describe "$INSTANCE" \
           --zone="$ZONE" \
           --format="value(status)")

  if [ "$STATUS" == "RUNNING" ]; then
    echo "实例 $INSTANCE 正在运行，准备停止..."
    gcloud compute instances stop "$INSTANCE" --zone="$ZONE"
  else
    echo "实例 $INSTANCE 当前状态：$STATUS，无需停止。"
  fi
  echo "--------------------------------------------------"
done

echo "全部实例已检查完毕。"
