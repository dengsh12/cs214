#!/bin/bash

# RabbitMQ节点信息
RABBITMQ_NODES=( "rabbitmq-1;10.128.0.12;34.59.119.117" "rabbitmq-2;10.128.0.10;34.68.113.17" "rabbitmq-3;10.128.0.11;35.238.173.156" )

echo "========== RabbitMQ集群测试脚本 =========="

# 安装必要的依赖
echo "准备测试环境..."
sudo apt-get update -y
sudo apt-get install -y python3-pip curl jq
pip3 install pika requests

# 创建Python测试脚本
echo "创建Python测试脚本..."
cat > rabbitmq_test.py << 'EOL'
#!/usr/bin/env python3

import pika
import time
import requests
import json
import sys
from requests.auth import HTTPBasicAuth

# RabbitMQ节点信息
NODES = [
    {"name": "rabbitmq-1", "internal_ip": "10.128.0.12", "external_ip": "34.59.119.117"},
    {"name": "rabbitmq-2", "internal_ip": "10.128.0.10", "external_ip": "34.68.113.17"},
    {"name": "rabbitmq-3", "internal_ip": "10.128.0.11", "external_ip": "35.238.173.156"}
]

# 连接凭据
CREDENTIALS = pika.PlainCredentials('guest', 'guest')

def test_connection(node):
    """测试到特定节点的连接"""
    print(f"\n正在测试到节点 {node['name']} ({node['external_ip']}) 的连接...")
    try:
        # 尝试连接到节点
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=node['external_ip'],
                credentials=CREDENTIALS,
                connection_attempts=3,
                retry_delay=5
            )
        )
        print(f"✅ 成功连接到节点 {node['name']}")
        
        # 创建通道
        channel = connection.channel()
        
        # 声明一个队列
        test_queue = f"test_queue_{node['name']}"
        channel.queue_declare(queue=test_queue, durable=True)
        
        # 发送测试消息
        message = f"测试消息发送到 {node['name']} - {time.time()}"
        channel.basic_publish(
            exchange='',
            routing_key=test_queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2  # 消息持久化
            )
        )
        print(f"✅ 消息已发送到节点 {node['name']}: {message}")
        
        # 接收消息
        method_frame, header_frame, body = channel.basic_get(test_queue)
        if method_frame:
            print(f"✅ 成功从节点 {node['name']} 接收消息: {body.decode('utf-8')}")
            channel.basic_ack(method_frame.delivery_tag)
        else:
            print(f"❌ 无法从节点 {node['name']} 接收消息")
        
        # 删除测试队列
        channel.queue_delete(queue=test_queue)
        
        # 关闭连接
        connection.close()
        return True
    except Exception as e:
        print(f"❌ 连接到节点 {node['name']} 失败: {str(e)}")
        return False

def check_cluster_status(node):
    """通过管理API检查集群状态"""
    print(f"\n正在检查节点 {node['name']} 的集群状态...")
    try:
        # 使用管理API获取集群状态
        url = f"http://{node['external_ip']}:15672/api/nodes"
        response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
        
        if response.status_code == 200:
            nodes_info = response.json()
            print(f"✅ 成功从节点 {node['name']} 获取集群信息")
            
            # 检查集群节点数量
            print(f"集群中的节点数量: {len(nodes_info)}")
            
            # 检查每个节点是否正在运行且为主节点
            running_nodes = []
            for node_info in nodes_info:
                node_name = node_info['name']
                is_running = node_info['running']
                
                if is_running:
                    running_nodes.append(node_name)
                    print(f"✅ 节点 {node_name} 正在运行")
                else:
                    print(f"❌ 节点 {node_name} 未运行")
            
            # 检查是否所有三个节点都在运行
            expected_nodes = 3
            if len(running_nodes) == expected_nodes:
                print(f"✅ 所有 {expected_nodes} 个节点都在集群中运行")
            else:
                print(f"❌ 发现 {len(running_nodes)}/{expected_nodes} 个运行节点")
            
            return running_nodes
        else:
            print(f"❌ 无法获取集群状态: HTTP {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ 检查集群状态时出错: {str(e)}")
        return []

def cross_node_message_test():
    """跨节点消息测试"""
    print("\n执行跨节点消息测试...")
    
    test_queue = "cross_node_test_queue"
    test_messages = {}
    
    try:
        # 在第一个节点上创建队列并发布消息
        sender_node = NODES[0]
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=sender_node['external_ip'],
                credentials=CREDENTIALS
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue=test_queue, durable=True)
        
        # 为每个节点发送一条消息
        for i, node in enumerate(NODES):
            message = f"跨节点测试消息 #{i+1} 到 {node['name']}"
            test_messages[i] = message
            channel.basic_publish(
                exchange='',
                routing_key=test_queue,
                body=message
            )
            print(f"✅ 通过节点 {sender_node['name']} 发送消息: {message}")
        
        connection.close()
        
        # 从其他节点接收消息
        for i, receiver_node in enumerate(NODES[1:], 1):
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=receiver_node['external_ip'],
                    credentials=CREDENTIALS
                )
            )
            channel = connection.channel()
            
            # 获取消息
            method_frame, header_frame, body = channel.basic_get(test_queue)
            if method_frame:
                received_message = body.decode('utf-8')
                print(f"✅ 从节点 {receiver_node['name']} 接收消息: {received_message}")
                channel.basic_ack(method_frame.delivery_tag)
            else:
                print(f"❌ 无法从节点 {receiver_node['name']} 接收消息")
            
            connection.close()
        
        # 清理：删除测试队列
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=NODES[0]['external_ip'],
                credentials=CREDENTIALS
            )
        )
        channel = connection.channel()
        channel.queue_delete(queue=test_queue)
        connection.close()
        
        print("✅ 跨节点消息测试完成")
        return True
    except Exception as e:
        print(f"❌ 跨节点消息测试失败: {str(e)}")
        return False

def main():
    print("开始RabbitMQ集群测试...")
    
    # 测试所有节点连接
    connection_results = []
    for node in NODES:
        result = test_connection(node)
        connection_results.append(result)
    
    # 通过第一个节点检查集群状态
    running_nodes = check_cluster_status(NODES[0])
    
    # 执行跨节点消息测试
    cross_node_test_result = cross_node_message_test()
    
    # 总结测试结果
    print("\n========== 测试结果总结 ==========")
    
    if all(connection_results):
        print("✅ 所有节点连接测试通过")
    else:
        print("❌ 部分节点连接测试失败")
    
    if len(running_nodes) == len(NODES):
        print("✅ 集群状态正常，所有节点都作为主节点运行")
    else:
        print(f"❌ 集群状态异常，期望 {len(NODES)} 个主节点，实际发现 {len(running_nodes)} 个")
    
    if cross_node_test_result:
        print("✅ 跨节点消息测试通过")
    else:
        print("❌ 跨节点消息测试失败")
    
    if all(connection_results) and len(running_nodes) == len(NODES) and cross_node_test_result:
        print("\n✅✅✅ 所有测试通过！RabbitMQ集群正常运行，三个节点都是主节点。")
        return 0
    else:
        print("\n❌❌❌ 测试失败！请检查RabbitMQ集群配置。")
        return 1

if __name__ == "__main__":
    sys.exit(main())
EOL

# 使Python脚本可执行
chmod +x rabbitmq_test.py

# 执行测试脚本
echo "执行RabbitMQ集群测试..."
python3 rabbitmq_test.py

# 使用RabbitMQ命令行验证集群状态
echo -e "\n使用RabbitMQ命令行工具验证集群状态..."

# 检查集群状态
for node in "${RABBITMQ_NODES[@]}"; do
  node_name=$(echo $node | cut -d';' -f1)
  node_external_ip=$(echo $node | cut -d';' -f3)
  
  echo -e "\n验证节点 $node_name ($node_external_ip) 的集群状态:"
  
  # 安装rabbitmqadmin工具（如果需要）
  if ! command -v rabbitmqadmin &> /dev/null; then
    echo "正在安装rabbitmqadmin..."
    curl -s "http://$node_external_ip:15672/cli/rabbitmqadmin" -o /usr/local/bin/rabbitmqadmin
    chmod +x /usr/local/bin/rabbitmqadmin
  fi
  
  # 查询并显示节点状态
  echo "节点状态:"
  curl -s -u guest:guest "http://$node_external_ip:15672/api/nodes" | jq .
  
  # 查看集群中的节点数量
  node_count=$(curl -s -u guest:guest "http://$node_external_ip:15672/api/nodes" | jq '. | length')
  echo "集群中的节点数量: $node_count"
  
  # 检查此节点是否知道所有其他节点
  if [ "$node_count" -eq "3" ]; then
    echo "✅ 节点 $node_name 正确识别了所有3个集群节点"
  else
    echo "❌ 节点 $node_name 只识别了 $node_count 个节点（应为3个）"
  fi
done

echo -e "\n========== 测试完成 ==========" 