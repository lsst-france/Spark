#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import matplotlib.lines as lines

defs = []

with open('G:\\mongo\\j.txt', 'rb') as f:
    for line in f:
        line = line.strip().decode('utf-8')
        line = line.strip('"')
        if len(line) == 0:
            continue
        if not line.startswith('id1'):
            continue
        words = line.split(' ')
        for word in words:
            word = word.strip()
            key, value = word.split(':')
            # print(key, value)
            if key == 'id1':
                id1 = value
            elif key == 'id2':
                id2 = value
            elif key == 'dist':
                dist = float(value)
            elif key == 'loc1':
                loc1 = eval(value)
            elif key == 'loc2':
                loc2 = eval(value)
        defs.append([id1, id2, dist, loc1, loc2])

if __name__ == '__main__':
    for d in defs:
        dist = d[2]
        loc1 = d[3]
        loc2 = d[4]

        if dist < 20:
            x = (loc1[0], loc2[0])
            y = (loc1[1], loc2[1])
            plt.plot(x, y, marker='o')

    plt.show()
