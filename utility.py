# utility.py
import time
import functools
import psutil
import threading

def timer(func):
    """装饰器：计算函数运行时间"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # 记录开始时间
        result = func(*args, **kwargs)
        end_time = time.time()  # 记录结束时间
        print(f"⏱️ {func.__name__} 执行耗时: {end_time - start_time:.6f} 秒")
        return result
    return wrapper

def resource_monitor(interval=1.0):
    """
    开始资源监控，返回采样列表，停止事件和监控线程
    """
    proc = psutil.Process()
    samples = []
    stop_event = threading.Event()

    def monitor():
        # 初始化cpu_percent计算
        proc.cpu_percent(interval=None)
        while not stop_event.is_set():
            cpu = proc.cpu_percent(interval=None)
            mem = proc.memory_info().rss
            samples.append((cpu, mem))
            time.sleep(interval)

    monitor_thread = threading.Thread(target=monitor)
    monitor_thread.start()
    return samples, stop_event, monitor_thread

def stop_resource_monitor(samples, stop_event, monitor_thread):
    """
    停止资源监控，并计算平均CPU和内存使用
    """
    stop_event.set()
    monitor_thread.join()
    if samples:
        avg_cpu = sum(sample[0] for sample in samples) / len(samples)
        avg_mem = sum(sample[1] for sample in samples) / len(samples)
    else:
        avg_cpu, avg_mem = 0, 0
    return avg_cpu, avg_mem
