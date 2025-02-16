clear
docker rm -f kafka  # 强制删除 Kafka 容器
docker rm -f kafka-zookeeper-1 kafka-kafka-1 
pip install confluent-kafka
docker compose up -d

echo "Sleep 5s, waiting for Kafka to start..."
sleep 5 # 等待 Kafka 容器启动完成，如果还没启动就python main.py会连接不上

python main.py
