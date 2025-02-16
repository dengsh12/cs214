import time
import functools

def timer(func):
    """装饰器：计算函数运行时间"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # 记录开始时间
        result = func(*args, **kwargs)  # 运行原函数
        end_time = time.time()  # 记录结束时间
        print(f"⏱️ {func.__name__} 执行耗时: {end_time - start_time:.6f} 秒")
        return result
    return wrapper