#!/bin/bash

echo "" > a.log
# 循环 50 次
for ((i=1; i<=100; i++))
do
    current_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo "test ${i} ${current_time=$(date "+%Y-%m-%d %H:%M:%S")}"
    echo "test ${i} ${current_time=$(date "+%Y-%m-%d %H:%M:%S")}" >> a.log
    go test -race -run 3B >> a.log
done