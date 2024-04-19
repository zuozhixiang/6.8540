import os
import time
import sys

# 从命令行中获取测试名称和测试次数
race = sys.argv[1]
test_name = sys.argv[2]
count = int(sys.argv[3])

# 定义变量，表示最大，最小和总时间
max_time = 0
min_time = 10000000
total_time = 0

# 循环运行测试命令
x = 0
for i in range(count):
    # 获取开始时间
    x += 1
    start = time.time()
    print(f'test {i} start')
    # 运行测试命令
    os.popen(f"go test {race} -run {test_name} > c.log").read()
    cnt = os.popen(f"ag FAIL c.log | wc -l").read().strip()
    if int(cnt) > 0:
        print(cnt)
        break

    # 获取结束时间
    end = time.time()
    print(f'test {i} end cost {end - start}')

    # 计算时间间隔
    duration = end - start

    # 更新最大和最小时间
    if duration > max_time:
        max_time = duration
    if duration < min_time:
        min_time = duration

    # 累加总时间
    total_time += duration

# 计算平均时间
average_time = total_time / x

# 输出结果
print(f"最大时间是：{max_time:.4f} 秒")
print(f"最小时间是：{min_time:.4f} 秒")
print(f"平均时间是：{average_time:.4f} 秒")