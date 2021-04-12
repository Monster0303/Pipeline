from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import threading
import time


def test_func(s, key):
    print(f'enter~~{threading.current_thread()} {s}s key={key}')
    threading.Event().wait(s)
    if key == 3:
        raise Exception(f"{key} failed~~~")
    return f'ok {threading.current_thread()}'


futures = {}


def run(fs):
    print('~~~~~~~~~~~~~~')
    while True:
        time.sleep(1)
        print('-' * 30)
        print('fs : ', fs)
        # 只要有一个任务没完成就会阻塞在这，
        # 当任意一个任务完成，就抛出一次
        # 如果内部有异常，result() 会将这个异常抛出
        # 有异常也算执行完了 complete
        # fs 内的任务都执行完了就不会阻塞了
        # fs 为空也不阻塞
        for future in as_completed(fs):
            id = fs[future]
            try:
                print(id, future.result())
            except Exception as e:
                print(e)
                print(id, 'failed')


threading.Thread(target=run, args=(futures,)).start()

time.sleep(5)  # 等 5 秒后开始提交任务

# executor = ThreadPoolExecutor(max_workers=3)  # 没使用上下文
# 使用上下文，在退出时清理线程
with ThreadPoolExecutor(max_workers=3) as executor:  # 池最大运行 3 个线程
    for i in range(7):  # 准备提交 7 个任务到池
        futures[executor.submit(test_func, random.randint(1, 8), i)] = i  # 生成任务
