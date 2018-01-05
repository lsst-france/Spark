#!/bin/bash

# spak.rpc.message.maxSize 200m

spark-submit --total-executor-cores 100 --driver-memory 12g --executor-memory 12g  FitsToHDFS.py


