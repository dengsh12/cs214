import os
import json
import re
import matplotlib.pyplot as plt

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
    metrics = [
        'consumer_avg_latency',
        'consumer_p99_latency',
        'producer_avg_throughput',
        'consumer_avg_throughput',
        'remote_avg_cpu',
        'remote_avg_mem_mb'
    ]
    
    for metric in metrics:
        data[metric] = {}

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):
            params = extract_params_from_filename(file_name)
            if params:
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r') as f:
                    content = json.load(f)
                    message_size = params['message_size']
                    
                    for metric in metrics:
                        if metric not in data:
                            data[metric] = {}
                        if message_size not in data[metric]:
                            data[metric][message_size] = []
                        
                        try:
                            metric_value = content['summary'][metric]
                            data[metric][message_size].append(
                                (params['num_consumers'], metric_value)
                            )
                        except KeyError as e:
                            print(f"Missing key {e} in file: {file_name}")
    
    # 对每个指标的数据按消费者数量排序
    for metric in metrics:
        for message_size in data[metric]:
            data[metric][message_size].sort(key=lambda x: x[0])
    
    return data

def plot_metric(data, metric_name, output_file, title, ylabel):
    """绘制单个指标的图表"""
    plt.figure(figsize=(12, 8))
    
    for message_size, values in data.items():
        consumers = [v[0] for v in values]
        metric_values = [v[1] for v in values]
        plt.plot(consumers, metric_values, marker='o', label=f'Message Size {message_size}')

    plt.title(title)
    plt.xlabel('Number of Consumers')
    plt.ylabel(ylabel)
    plt.legend(title="Message Size")
    plt.grid(True)
    plt.savefig(output_file, format='png', dpi=300)
    plt.close()

def plot_all_metrics(folder_path, output_dir):
    """绘制所有指标的图表"""
    create_output_dir(output_dir)
    data = load_data(folder_path)
    
    metrics_config = {
        'consumer_avg_latency': {
            'title': 'Consumer Average Latency vs Number of Consumers',
            'ylabel': 'Average Consumer Latency (ms)'
        },
        'consumer_p99_latency': {
            'title': 'Consumer P99 Latency vs Number of Consumers',
            'ylabel': 'P99 Consumer Latency (ms)'
        },
        'producer_avg_throughput': {
            'title': 'Producer Average Throughput vs Number of Consumers',
            'ylabel': 'Average Producer Throughput (msg/s)'
        },
        'consumer_avg_throughput': {
            'title': 'Consumer Average Throughput vs Number of Consumers',
            'ylabel': 'Average Consumer Throughput (msg/s)'
        },
        'remote_avg_cpu': {
            'title': 'Remote Average CPU Usage vs Number of Consumers',
            'ylabel': 'Average CPU Usage'
        },
        'remote_avg_mem_mb': {
            'title': 'Remote Average Memory Usage vs Number of Consumers',
            'ylabel': 'Average Memory Usage (MB)'
        }
    }
    
    for metric, config in metrics_config.items():
        output_file = os.path.join(output_dir, f'{metric}.png')
        plot_metric(
            data[metric],
            metric,
            output_file,
            config['title'],
            config['ylabel']
        )
        print(f"已保存图表: {output_file}")

if __name__ == "__main__":
    input_folder = '/home/songh00/zytTemp/cs214/results/kafka'
    output_dir = '/home/songh00/zytTemp/cs214/draw_picture/metrics_plots'
    plot_all_metrics(input_folder, output_dir)