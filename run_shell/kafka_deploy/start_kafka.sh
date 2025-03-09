#!/usr/bin/env bash

# 需要检查的实例名称列表（与实际名称对应）
INSTANCES=("mq-1" "mq-2" "mq-3")
# 所在的 Zone，如果这三个实例不在同一个 zone，需要分别指定
ZONE="us-central1-c"

for i in "${!INSTANCES[@]}"; do
  INSTANCE_NAME="${INSTANCES[$i]}"
  # 脚本中要传的参数 k（第 i+1 个实例）
  KAFKA_ID=$((i + 1))
  
  echo "正在检查实例 $INSTANCE_NAME 的状态..."
  # 获取实例当前状态（如 RUNNING, TERMINATED, STOPPED 等）
  STATUS=$(gcloud compute instances describe "$INSTANCE_NAME" \
           --zone="$ZONE" \
           --format="value(status)")

  # 如果状态不是 RUNNING，则先启动
  if [ "$STATUS" != "RUNNING" ]; then
    echo "实例 $INSTANCE_NAME 当前状态：$STATUS，正在启动..."
    gcloud compute instances start "$INSTANCE_NAME" --zone="$ZONE"
    # 因为启动需要一定时间，这里最好等待片刻或者轮询判断
    echo "等待实例启动中..."
    sleep 30
  else
    echo "实例 $INSTANCE_NAME 已处于运行状态。"
  fi

  # 通过 gcloud compute ssh 在对应实例上执行脚本：
  # 1) 运行 kafka_setup.sh
  # 2) 检查 5000 端口，如未占用则后台启动 remote_agent.py
  #    （使用 nohup + &，并将日志重定向到 /home/cs214/agent.log）
  echo "在 $INSTANCE_NAME 上执行 kafka_setup.sh，参数为 $KAFKA_ID..."
  gcloud compute ssh "$INSTANCE_NAME" \
    --zone="$ZONE" \
    --command="
      echo '--- 运行 kafka_setup.sh ---';
      bash /home/cs214/kafka_setup.sh $KAFKA_ID;

      echo '--- 检查端口 5000 是否空闲 ---';
      port_in_use=\$(sudo lsof -t -i:5000);
      if [ -z \"\$port_in_use\" ]; then
        echo 'Port 5000 is free. 准备后台启动 remote_agent.py...';
        source /home/cs214/venv/bin/activate;
        nohup python /home/cs214/remote_agent.py > /home/cs214/agent.log 2>&1 &
        echo 'remote_agent.py 已在后台运行，日志写入 /home/cs214/agent.log。'
      else
        echo \"Port 5000 已被进程 \$port_in_use 占用，跳过启动 remote_agent.py。\"
      fi
    "
done
