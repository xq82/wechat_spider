from multiprocessing.pool import ThreadPool
import time
import random
"""测试多线程"""
def main():
    print("111111111111111111")
    a = random.randint(0,5)
    print(a)
    time.sleep(a)

pool = ThreadPool(100)
for i in range(100):
    pool.apply_async(main)
pool.close()
pool.join()
print("aaaaaaaaaaaaaaaa")