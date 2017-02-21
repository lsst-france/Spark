#!/usr/bin/env python
# -*- coding: utf-8 -*-

from joblib import Parallel, delayed
import multiprocessing
import time

def processInput(i):
    j = 1
    for k in range(100000):
        j *= i
    return j

if __name__ == '__main__':
    # what are your inputs, and what operation do you want to
    # perform on each input. For example...

    inputs = range(10)

    num_cores = multiprocessing.cpu_count()
    print(num_cores)
    time1 = time.time()
    results = Parallel(n_jobs=1)(delayed(processInput)(i) for i in inputs)
    time2 = time.time()

    print('all objects done', time2-time1)

    print(results)