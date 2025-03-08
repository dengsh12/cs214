# rocketmq_admin.py
import os
import subprocess

# 修改为你本地真实的 mqadmin 路径
MQADMIN_PATH = "/home/songh00/rocketmq_temp/rocketmq-all-4.9.8-bin-release/bin/mqadmin"
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"

# 如果需要为 JDK17 添加 opens 等参数，请放到这里
JVM_OPTS = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"
)

def run_mqadmin_command(cmd_list):
    env = os.environ.copy()
    env['JAVA_HOME'] = JAVA_HOME
    existing_opts = env.get('JAVA_TOOL_OPTIONS', '')
    if JVM_OPTS not in existing_opts:
        env['JAVA_TOOL_OPTIONS'] = (existing_opts + " " + JVM_OPTS).strip()
    subprocess.check_call(cmd_list, env=env)

def delete_topic_rocketmq(topic_name, namesrv_addr):
    """
    删除 RocketMQ Topic
    :param topic_name: 待删除的 Topic
    :param namesrv_addr: "ip1:9876;ip2:9876;ip3:9876"
    """
    print(f"尝试删除RocketMQ Topic: {topic_name}")
    cmd = [
        MQADMIN_PATH,
        "deleteTopic",
        "-n", namesrv_addr,
        "-c", "DefaultCluster",
        "-t", topic_name
    ]
    try:
        run_mqadmin_command(cmd)
        print(f"✅ RocketMQ Topic {topic_name} 删除成功")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ RocketMQ Topic {topic_name} 删除失败或不存在: {e}")

def create_topic_rocketmq(topic_name, namesrv_addr, num_queues=8):
    """
    创建 RocketMQ Topic
    :param topic_name: Topic 名称
    :param namesrv_addr: "ip1:9876;ip2:9876;ip3:9876"
    :param num_queues: 创建时的读写队列数
    """
    print(f"🚀 创建 RocketMQ Topic: {topic_name}, queues={num_queues}")
    cmd = [
        MQADMIN_PATH,
        "updateTopic",
        "-n", namesrv_addr,
        "-c", "DefaultCluster",
        "-t", topic_name,
        "-w", str(num_queues),  # 写队列数
        "-r", str(num_queues),  # 读队列数
        "-p", "6"               # 读写权限(2=写,4=读,6=读写)
    ]
    try:
        run_mqadmin_command(cmd)
        print(f"✅ RocketMQ Topic {topic_name} 创建成功")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ RocketMQ Topic {topic_name} 创建失败: {e}")
