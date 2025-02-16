docker rm -f kafka-zookeeper-1 kafka-kafka-1 
docker compose up -d

sleep_time=5
echo "\nSleep $sleep_time s, waiting for Kafka to start...\n"

docker ps
sleep $sleep_time
# 等待 Kafka 容器启动完成，如果还没启动就python main.py会连接不上
docker ps
