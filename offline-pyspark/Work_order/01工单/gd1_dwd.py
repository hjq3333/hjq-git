# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("gd01 DWD Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "D:/spark_temp") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.sql.parquet.binaryAsString", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置当前数据库
spark.sql("USE gd01")

print("开始处理gd01 DWD层数据...")

# 工具函数定义
def create_hdfs_dir(path):
    """创建HDFS目录"""
    from py4j.java_gateway import java_import
    java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
    java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

def repair_hive_table(table_name):
    """修复Hive表分区"""
    spark.sql(f"MSCK REPAIR TABLE gd01.{table_name}")
    print(f"修复分区完成：gd01.{table_name}")

# ====================== DWD层商品访问事实表 ======================
print("处理商品访问事实表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dwd/dwd_product_view")

# # 2. 删除旧表
# spark.sql("DROP TABLE IF EXISTS dwd_product_view")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE if not exists dwd_product_view
(
    `product_id`   STRING COMMENT '商品ID',
    `user_id`      STRING COMMENT '用户ID',
    `visit_date`   STRING COMMENT '访问日期',
    `platform`     STRING COMMENT '平台（PC/无线）',
    `view_count`   BIGINT COMMENT '访问次数',
    `visitor_count` BIGINT COMMENT '访客数'
) COMMENT '商品访问事实表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dwd/dwd_product_view'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 读取ODS层数据
print("读取ODS层商品访问日志数据...")
ods_view_log = spark.sql("""
    SELECT * FROM ods_product_view_log 
    WHERE dt = '20250808' 
    AND product_id IS NOT NULL 
    AND user_id IS NOT NULL
""")

# 5. 数据处理，确保字段类型正确
print("处理商品访问数据...")
dwd_product_view = ods_view_log.select(
    F.col("product_id").cast("string").alias("product_id"),
    F.col("user_id").cast("string").alias("user_id"),
    F.date_format(F.col("visit_time"), "yyyy-MM-dd").cast("string").alias("visit_date"),
    F.col("platform").cast("string").alias("platform"),
    F.lit(1).cast("long").alias("view_count"),  # 每条记录代表一次访问
    F.lit(1).cast("long").alias("visitor_count"),  # 每条记录代表一个访客
    F.lit("20250808").cast("string").alias("dt")
)

# 6. 写入数据
print("写入商品访问事实表数据...")
dwd_product_view.write.mode("append") \
    .option("parquet.writelegacyformat", "true") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dwd/dwd_product_view")

# 7. 修复分区
repair_hive_table("dwd_product_view")

print(f"商品访问事实表处理完成，记录数: {dwd_product_view.count()}")

# ====================== DWD层商品收藏事实表 ======================
print("处理商品收藏事实表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dwd/dwd_product_collect")

# # 2. 删除旧表
# spark.sql("DROP TABLE IF EXISTS dwd_product_collect")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE if not exists dwd_product_collect
(
    `product_id`     STRING COMMENT '商品ID',
    `user_id`        STRING COMMENT '用户ID',
    `collect_date`   STRING COMMENT '收藏日期',
    `collector_count` BIGINT COMMENT '收藏人数'
) COMMENT '商品收藏事实表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dwd/dwd_product_collect'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 读取ODS层数据
print("读取ODS层商品收藏日志数据...")
ods_collect_log = spark.sql("""
    SELECT * FROM ods_product_collect_log 
    WHERE dt = '20250808' 
    AND product_id IS NOT NULL 
    AND user_id IS NOT NULL
""")

# 5. 数据处理，确保字段类型正确
print("处理商品收藏数据...")
dwd_product_collect = ods_collect_log.select(
    F.col("product_id").cast("string").alias("product_id"),
    F.col("user_id").cast("string").alias("user_id"),
    F.date_format(F.col("collect_time"), "yyyy-MM-dd").cast("string").alias("collect_date"),
    F.lit(1).cast("long").alias("collector_count"),  # 每条记录代表一个收藏用户
    F.lit("20250808").cast("string").alias("dt")
)

# 6. 写入数据
print("写入商品收藏事实表数据...")
dwd_product_collect.write.mode("append") \
    .option("parquet.writelegacyformat", "true") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dwd/dwd_product_collect")

# 7. 修复分区
repair_hive_table("dwd_product_collect")

print(f"商品收藏事实表处理完成，记录数: {dwd_product_collect.count()}")

# ====================== DWD层商品加购事实表 ======================
print("处理商品加购事实表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dwd/dwd_product_cart")

# # 2. 删除旧表
# spark.sql("DROP TABLE IF EXISTS dwd_product_cart")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE if not exists dwd_product_cart
(
    `product_id`      STRING COMMENT '商品ID',
    `user_id`         STRING COMMENT '用户ID',
    `cart_date`       STRING COMMENT '加购日期',
    `cart_quantity`   BIGINT COMMENT '加购件数',
    `cart_user_count` BIGINT COMMENT '加购用户数'
) COMMENT '商品加购事实表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dwd/dwd_product_cart'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 读取ODS层数据
print("读取ODS层商品加购日志数据...")
ods_cart_log = spark.sql("""
    SELECT * FROM ods_product_cart_log 
    WHERE dt = '20250808' 
    AND product_id IS NOT NULL 
    AND user_id IS NOT NULL
""")

# 5. 数据处理，确保字段类型正确
print("处理商品加购数据...")
dwd_product_cart = ods_cart_log.select(
    F.col("product_id").cast("string").alias("product_id"),
    F.col("user_id").cast("string").alias("user_id"),
    F.date_format(F.col("add_time"), "yyyy-MM-dd").cast("string").alias("cart_date"),
    F.col("quantity").cast("long").alias("cart_quantity"),
    F.lit(1).cast("long").alias("cart_user_count"),  # 每条记录代表一个加购用户
    F.lit("20250808").cast("string").alias("dt")
)

# 6. 写入数据
print("写入商品加购事实表数据...")
dwd_product_cart.write.mode("append") \
    .option("parquet.writelegacyformat", "true") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dwd/dwd_product_cart")

# 7. 修复分区
repair_hive_table("dwd_product_cart")

print(f"商品加购事实表处理完成，记录数: {dwd_product_cart.count()}")

# ====================== DWD层商品订单事实表 ======================
print("处理商品订单事实表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dwd/dwd_product_order")

# # 2. 删除旧表
# spark.sql("DROP TABLE IF EXISTS dwd_product_order")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE if not exists dwd_product_order
(
    `product_id`        STRING COMMENT '商品ID',
    `user_id`           STRING COMMENT '用户ID',
    `order_date`        STRING COMMENT '订单日期',
    `quantity`          BIGINT COMMENT '订单件数',
    `amount`            DECIMAL(10,2) COMMENT '订单金额',
    `paid_quantity`     BIGINT COMMENT '支付件数',
    `paid_amount`       DECIMAL(10,2) COMMENT '支付金额',
    `order_user_count`  BIGINT COMMENT '下单用户数',
    `paid_user_count`   BIGINT COMMENT '支付用户数'
) COMMENT '商品订单事实表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dwd/dwd_product_order'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 读取ODS层数据
print("读取ODS层商品订单日志数据...")
ods_order_log = spark.sql("""
    SELECT * FROM ods_product_order_log 
    WHERE dt = '20250808' 
    AND product_id IS NOT NULL 
    AND user_id IS NOT NULL
""")

# 5. 数据处理，确保字段类型正确
print("处理商品订单数据...")
dwd_product_order = ods_order_log.select(
    F.col("product_id").cast("string").alias("product_id"),
    F.col("user_id").cast("string").alias("user_id"),
    F.date_format(F.col("order_time"), "yyyy-MM-dd").cast("string").alias("order_date"),
    F.col("quantity").cast("long").alias("quantity"),
    F.col("amount").cast("decimal(10,2)").alias("amount"),
    # 根据is_paid字段判断是否支付 (0:已支付 1:未支付)
    F.when(F.col("is_paid") == 0, F.col("quantity")).otherwise(F.lit(0)).cast("long").alias("paid_quantity"),
    F.when(F.col("is_paid") == 0, F.col("amount")).otherwise(F.lit(0.0)).cast("decimal(10,2)").alias("paid_amount"),
    F.lit(1).cast("long").alias("order_user_count"),  # 每条记录代表一个下单用户
    F.when(F.col("is_paid") == 0, F.lit(1)).otherwise(F.lit(0)).cast("long").alias("paid_user_count"),
    F.lit("20250808").cast("string").alias("dt")
)

# 6. 写入数据
print("写入商品订单事实表数据...")
dwd_product_order.write.mode("append") \
    .option("parquet.writelegacyformat", "true") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dwd/dwd_product_order")

# 7. 修复分区
repair_hive_table("dwd_product_order")

print(f"商品订单事实表处理完成，记录数: {dwd_product_order.count()}")

# ====================== 数据验证 ======================
print("验证DWD层数据:")
try:
    view_count = spark.sql("SELECT COUNT(*) as count FROM dwd_product_view WHERE dt = '20250808'").collect()[0]['count']
    collect_count = spark.sql("SELECT COUNT(*) as count FROM dwd_product_collect WHERE dt = '20250808'").collect()[0]['count']
    cart_count = spark.sql("SELECT COUNT(*) as count FROM dwd_product_cart WHERE dt = '20250808'").collect()[0]['count']
    order_count = spark.sql("SELECT COUNT(*) as count FROM dwd_product_order WHERE dt = '20250808'").collect()[0]['count']
    print(f"商品访问事实表记录数: {view_count}")
    print(f"商品收藏事实表记录数: {collect_count}")
    print(f"商品加购事实表记录数: {cart_count}")
    print(f"商品订单事实表记录数: {order_count}")

    # 展示部分数据样本
    print("商品访问事实表样本数据:")
    spark.sql("SELECT * FROM dwd_product_view WHERE dt = '20250808' LIMIT 5").show(truncate=False)

    print("商品收藏事实表样本数据:")
    spark.sql("SELECT * FROM dwd_product_collect WHERE dt = '20250808' LIMIT 5").show(truncate=False)

    print("商品加购事实表样本数据:")
    spark.sql("SELECT * FROM dwd_product_cart WHERE dt = '20250808' LIMIT 5").show(truncate=False)

    print("商品订单事实表样本数据:")
    spark.sql("SELECT * FROM dwd_product_order WHERE dt = '20250808' LIMIT 5").show(truncate=False)

except Exception as e:
    print(f"验证DWD层数据时出错: {e}")

print("gd01 DWD层数据处理完成!")
spark.stop()
