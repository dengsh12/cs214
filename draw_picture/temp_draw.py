import os
import json
import re
import matplotlib.pyplot as plt
import numpy as np

def create_output_dir(base_dir):
    """创建输出目录"""
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

def extract_params_from_filename(filename):
    """从文件名中提取参数"""
    pattern = re.compile(r'(\d+)consumer(\d+)producer(\d+)kmessages')
    match = pattern.search(filename)
    if match:
        return {
            'num_consumers': int(match.group(1)),
            'num_producers': int(match.group(2)),
            'message_size': int(match.group(3)) * 1000
        }
    return None

def load_data(folder_path):
    """加载所有JSON文件的数据"""
    data = {}
    metric = 'consumer_avg_throughput'
    
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):
            params = extract_params_from_filename(file_name)
            if params:
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r') as f:
                    content = json.load(f)
                    message_size = params['message_size']
                    
                    if message_size not in data:
                        data[message_size] = []
                    
                    try:
                        metric_value = content['summary'][metric]
                        data[message_size].append((params['num_consumers'], metric_value))
                    except KeyError as e:
                        print(f"Missing key {e} in file: {file_name}")
    
    # 对数据按消费者数量排序
    for message_size in data:
        data[message_size].sort(key=lambda x: x[0])
    
    return data

def plot_consumer_throughput(data, output_file):
    """绘制消费者平均吞吐量，并添加 y = 2000 / x 参考线"""
    plt.figure(figsize=(12, 8))
    
    for message_size, values in data.items():
        consumers = [v[0] for v in values]
        throughput_values = [v[1] for v in values]
        plt.plot(consumers, throughput_values, marker='o', label=f'Message Size {message_size}')
    
    # 添加 y = 2000 / x 参考线
    x_vals = np.linspace(min(consumers), max(consumers), 100)
    y_vals = 2000 / x_vals
    plt.plot(x_vals, y_vals, 'r--', label='y = 2000 / x')
    
    plt.title('Consumer Average Throughput vs Number of Consumers')
    plt.xlabel('Number of Consumers')
    plt.ylabel('Average Consumer Throughput (msg/s)')
    plt.legend(title="Message Size")
    plt.grid(True)
    plt.savefig(output_file, format='png', dpi=300)
    plt.close()

def main():
    input_folder = '/home/songh00/zytTemp/cs214/results/rocketmq'
    output_dir = '/home/songh00/zytTemp/cs214/draw_picture/metrics_plots'
    create_output_dir(output_dir)
    
    data = load_data(input_folder)
    output_file = os.path.join(output_dir, 'consumer_avg_throughput.png')
    plot_consumer_throughput(data, output_file)
    print(f"已保存图表: {output_file}")

if __name__ == "__main__":
    main()
