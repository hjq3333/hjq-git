#!/bin/bash
export mysql_db=$1 hive_db=$2

# 内存检查函数
check_memory() {
  local threshold=80 # 内存使用率阈值%
  local used=$(free | awk '/Mem/{printf("%d"), $3/$2*100}')
  while [ $used -gt $threshold ]; do
    echo "内存使用率${used}%过高，等待释放..."
    sleep 5
    used=$(free | awk '/Mem/{printf("%d"), $3/$2*100}')
  done
}

tables=$(mysql -hcdh03 -uroot -proot $mysql_db -e "SHOW TABLES" | awk 'NR>1 {print $1}')

for table in $tables; do
  hive_table=$table
  echo "正在同步表 $table..."
  
  # 生成字段映射前先检查表结构
  mysql -hcdh03 -uroot -proot $mysql_db -e "DESC $table" > /tmp/fields.tmp
  if [ $? -ne 0 ]; then
    echo "获取表结构失败，跳过表 $table"
    continue
  fi

  # 生成标准化的fields.json
  awk 'NR>1 {print "\""$1"\""}' /tmp/fields.tmp | paste -sd "," - | sed 's/^/[\n/; s/$/\n]/' > fields.json

  sh bin/seatunnel.sh -c myconf/Mysql2Hive1.conf -m local \
    -i "mysql_db=${mysql_db}" \
    -i "mysql_table=${table}" \
    -i "hive_db=${hive_db}" \
    -i "hive_table=${hive_table}" \
    -i "fields=@fields.json"

  if [ $? -ne 0 ]; then
    echo "同步表 $table 失败"
    exit 1
  fi
  
  check_memory
  sleep 3 # 基础延迟
done
echo "所有表同步完成。"
