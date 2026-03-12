#!/bin/bash

# 检查 eno1d1是否存在且处于UP状态
if ip link show  eno1d1 &> /dev/null && \
   ip link show  eno1d1 | grep -q 'state UP'
then
    wondershaper -a  eno1d1 -d 1048576 -u 1048576
    exit 0
fi

# 检查enp6s0f1是否存在且处于UP状态
if ip link show enp6s0f1 &> /dev/null && \
   ip link show enp6s0f1 | grep -q 'state UP'
then
    wondershaper -a enp6s0f1 -d 1048576 -u 1048576
    exit 0
fi

# 如果都不满足则报错
echo "Error: No active interface found!" >&2
exit 1