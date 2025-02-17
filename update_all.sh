#!/bin/bash

cd /users/qiliang
sudo chmod 777 -R UniEC
cd UniEC


# 定义源文件夹路径
SOURCE_DIR="/users/qiliang/UniEC"

# 定义 hosts 文件路径
HOSTS_FILE="hosts"

# 定义远程目标文件夹路径
REMOTE_DIR="/users/qiliang/UniEC"

# 检查 hosts 文件是否存在
if [[ ! -f "$HOSTS_FILE" ]]; then
    echo "Error: hosts file not found!"
    exit 1
fi

# 遍历 hosts 文件中的每个 IP 地址
while read -r ip; do

    echo "Copying to host: $ip..."

    # 使用 rsync 复制文件夹，排除 third_party 子文件夹
    rsync -avz --exclude='third_party' -e ssh "$SOURCE_DIR/" "$ip:$REMOTE_DIR/"
    #rsync -avz -e ssh "$SOURCE_DIR/" "$ip:$REMOTE_DIR/"

    # 检查 rsync 是否成功
    if [ $? -eq 0 ]; then
        echo "Successfully copied to $ip!"
    else
        echo "Failed to copy to $ip!"
    fi

done < "$HOSTS_FILE"

cd /users/qiliang/UniEC
sh generate_run_proxy.sh

echo "All done!"