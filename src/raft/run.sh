#!/bin/bash


# 循环 50 次
for ((i=1; i<=5; i++))
do
    echo "" > a.log
    current_time=$(date "+%Y-%m-%d %H:%M:%S")
    echo "test ${i} ${current_time}"
    echo "test ${i} ${current_time}" >> a.log
    go test >> a.log
    fail_count=$(ag FAIL a.log | wc -l)
    if [ ${fail_count} -gt 0 ]
    then
      echo "" >> a.log
      echo "test ${i} fail $(date "+%Y-%m-%d %H:%M:%S")"
      break
    else
      echo "test ${i} success $(date "+%Y-%m-%d %H:%M:%S")"
    fi
done