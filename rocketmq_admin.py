import os
import subprocess
import re
import time

# 修改为你本地实际路径
MQADMIN_PATH = "/home/songh00/rocketmq_temp/rocketmq-all-4.9.8-bin-release/bin/mqadmin"
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"

JVM_OPTS = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"
)

def run_mqadmin_command(cmd_list):
    env = os.environ.copy()
    # 强行设置 JAVA_HOME，避免脚本报错
    env['JAVA_HOME'] = JAVA_HOME
    existing_opts = env.get('JAVA_TOOL_OPTIONS', '')
    if JVM_OPTS not in existing_opts:
        env['JAVA_TOOL_OPTIONS'] = (existing_opts + " " + JVM_OPTS).strip()
    # 调用
    subprocess.check_call(cmd_list, env=env)

def delete_topic_rocketmq(topic_name, namesrv_addr):
    # 解析 namesrv_addr，支持 , 或 ; 分隔
    addr_list = re.split(r'[;,]', namesrv_addr)
    
    print(f"\n开始彻底删除 RocketMQ Topic: {topic_name}")

    for addr in addr_list:
        addr = addr.strip()
        if not addr:
            continue
        
    # 删除Topic
    for addr in addr_list:
        addr = addr.strip()  # 去除空格，避免格式问题
        if not addr:
            continue
        print(f"\n正在删除 RocketMQ Topic: {topic_name}, NameServer: {addr}")
        
        # 2.1 先尝试从Broker删除
        cmd = [
            MQADMIN_PATH,
            "deleteTopic",
            "-n", addr,
            "-c", "DefaultCluster",
            "-t", topic_name
        ]
        try:
            run_mqadmin_command(cmd)
            print(f"✅ RocketMQ Topic {topic_name} 在 {addr} 删除成功（broker端）")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ RocketMQ Topic {topic_name} 在 {addr} 删除失败或不存在: {e}")
        
      
    # 等待确认删除完成
    print("等待删除操作完成...")
    time.sleep(3)
    print(f"✅ Topic {topic_name} 删除操作完成")


def create_topic_rocketmq(topic_name, namesrv_addr, num_queues=8):
    print(f"🚀 创建 RocketMQ Topic: {topic_name}, queues={num_queues}")
    cmd = [
        MQADMIN_PATH,
        "updateTopic",
        "-n", namesrv_addr,
        "-c", "DefaultCluster",
        "-t", topic_name,
        "-w", str(num_queues),
        "-r", str(num_queues),
        "-p", "6"
    ]
    try:
        run_mqadmin_command(cmd)
        print(f"✅ RocketMQ Topic {topic_name} 创建成功")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ RocketMQ Topic {topic_name} 创建失败: {e}")
