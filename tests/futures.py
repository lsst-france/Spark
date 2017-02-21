#!/usr/bin/env python
# -*- coding: utf-8 -*-

import concurrent.futures
import math
import time

PRIMES = [
    112272535095293,
    112582705942171,
    112272535095293,
    115280095190773,
    115797848077099,
    1099726899285419]

def is_prime(n):
    if n % 2 == 0:
        return False

    sqrt_n = int(math.floor(math.sqrt(n)))
    for i in range(3, sqrt_n + 1, 2):
        if n % i == 0:
            return -1
    return n

class Stepper(object):
    previous_time = None

    def __init__(self):
        self.previous_time = time.time()

    def show_step(self, label='Initial time'):
        now = time.time()
        delta = now - self.previous_time

        print('--------------------------------', label, '{:.3f} seconds'.format(delta))

        self.previous_time = now



def main():

    stepper = Stepper()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for number, prime in zip(PRIMES, executor.map(is_prime, PRIMES)):
            print('%d is prime: %s' % (number, prime))

    stepper.show_step('a')

    fs = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for prime in PRIMES:
            future = executor.submit(is_prime, prime)
            fs.append(future)

    for future in fs:
        print(future.result())

    stepper.show_step('b')

        # for number, prime in zip(PRIMES, results):
    #     print('%d is prime: %s' % (number, prime))

if __name__ == '__main__':
    main()

