#!/bin/bash

# 死循环，每 5 秒钟执行一次
while true
do

    current_time=`date`
    echo ${current_time}
    grep FAIL a.log
    sleep 140
done