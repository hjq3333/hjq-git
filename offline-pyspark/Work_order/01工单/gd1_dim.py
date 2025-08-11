# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("gd01 DIM Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "D:/spark_temp") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.sql.parquet.binaryAsString", "true") \
    .config("spark.sql.parquet.int96AsTimestamp", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置当前数据库
spark.sql("USE gd01")

print("开始处理gd01 DIM层数据...")

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
    try:
        spark.sql(f"MSCK REPAIR TABLE gd01.{table_name}")
        print(f"修复分区完成：gd01.{table_name}")
    except Exception as e:
        print(f"修复分区失败：gd01.{table_name}，错误：{e}")

def delete_hdfs_path(path):
    """删除HDFS路径"""
    from py4j.java_gateway import java_import
    java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
    java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    jvm_path = spark.sparkContext._jvm.Path(path)
    if fs.exists(jvm_path):
        fs.delete(jvm_path, True)
        print(f"已删除HDFS路径：{path}")
    else:
        print(f"HDFS路径不存在：{path}")

# ====================== DIM层商品维度表 ======================
print("处理商品维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dim/dim_product")

# # 2. 删除旧表和数据
# spark.sql("DROP TABLE IF EXISTS dim_product")
# delete_hdfs_path("/warehouse/gd01/dim/dim_product")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL   TABLE IF not EXISTS dim_product
(
    product_id      STRING COMMENT '商品ID',
    product_name    STRING COMMENT '商品名称',
    category_name   STRING COMMENT '类目名称',
    brand           STRING COMMENT '品牌',
    price           DECIMAL(10,2) COMMENT '商品价格',
    create_time     TIMESTAMP COMMENT '创建时间',
    update_time     TIMESTAMP COMMENT '更新时间'
) COMMENT '商品维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dim/dim_product'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入商品维度表数据
print("插入商品维度表数据...")
ods_product_info = spark.sql("SELECT * FROM ods_product_info WHERE dt = '20250806'")

# 使用更保守的数据处理方式
dim_product = ods_product_info.select(
    F.col("product_id").cast("string").alias("product_id"),
    F.coalesce(F.col("product_name"), F.lit("")).cast("string").alias("product_name"),
    F.coalesce(F.col("category_name"), F.lit("")).cast("string").alias("category_name"),
    F.coalesce(F.col("brand"), F.lit("")).cast("string").alias("brand"),
    F.coalesce(F.col("price"), F.lit(0.0)).cast("decimal(10,2)").alias("price"),
    F.col("create_time").cast("timestamp").alias("create_time"),
    F.col("operate_time").cast("timestamp").alias("update_time")
).filter(
    F.col("product_id").isNotNull()
).withColumn("dt", F.lit("20250806"))

# 写入数据
dim_product.write.mode("append") \
    .option("compression", "snappy") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dim/dim_product")

# 修复分区
repair_hive_table("dim_product")

print(f"商品维度表处理完成，记录数: {dim_product.count()}")

# ====================== DIM层用户维度表 ======================
print("处理用户维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dim/dim_user")

# # 2. 删除旧表和数据
# spark.sql("DROP TABLE IF EXISTS dim_user")
# delete_hdfs_path("/warehouse/gd01/dim/dim_user")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL   TABLE IF not EXISTS dim_user
(
    user_id       STRING COMMENT '用户ID',
    user_name     STRING COMMENT '用户名称',
    gender        STRING COMMENT '性别',
    level         STRING COMMENT '用户级别',
    create_time   TIMESTAMP COMMENT '创建时间',
    update_time   TIMESTAMP COMMENT '更新时间'
) COMMENT '用户维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dim/dim_user'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入用户维度表数据
print("插入用户维度表数据...")
ods_user_info = spark.sql("SELECT * FROM ods_user_info WHERE dt = '20250806'")

# 使用更保守的数据处理方式
dim_user = ods_user_info.select(
    F.col("user_id").cast("string").alias("user_id"),
    F.coalesce(F.col("user_name"), F.lit("")).cast("string").alias("user_name"),
    F.coalesce(F.col("gender"), F.lit("")).cast("string").alias("gender"),
    F.coalesce(F.col("level"), F.lit("")).cast("string").alias("level"),
    F.col("create_time").cast("timestamp").alias("create_time"),
    F.col("operate_time").cast("timestamp").alias("update_time")
).filter(
    F.col("user_id").isNotNull()
).withColumn("dt", F.lit("20250806"))

# 写入数据
dim_user.write.mode("append") \
    .option("compression", "snappy") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dim/dim_user")

# 修复分区
repair_hive_table("dim_user")

print(f"用户维度表处理完成，记录数: {dim_user.count()}")

# ====================== DIM层平台维度表 ======================
print("处理平台维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dim/dim_platform")

# # 2. 删除旧表和数据
# spark.sql("DROP TABLE IF EXISTS dim_platform")
# delete_hdfs_path("/warehouse/gd01/dim/dim_platform")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL   TABLE IF not EXISTS dim_platform
(
    platform_id   STRING COMMENT '平台ID',
    platform_name STRING COMMENT '平台名称',
    platform_desc STRING COMMENT '平台描述'
) COMMENT '平台维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dim/dim_platform'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入平台维度表数据
print("插入平台维度表数据...")
# 仅从商品访问日志中提取平台信息（因为订单表中没有platform字段）
platform_ids_from_view = spark.sql("""
    SELECT DISTINCT platform 
    FROM ods_product_view_log 
    WHERE dt = '20250806' AND platform IS NOT NULL
""")

# 创建平台维度数据
dim_platform = platform_ids_from_view.select(
    F.col("platform").cast("string").alias("platform_id"),
    F.col("platform").cast("string").alias("platform_name"),
    F.when(F.col("platform").cast("string") == "PC", "PC端")
    .when(F.col("platform").cast("string") == "无线", "移动端")
    .otherwise(F.concat(F.lit("平台_"), F.col("platform").cast("string")))
    .cast("string").alias("platform_desc")
).filter(
    F.col("platform").isNotNull()
).withColumn("dt", F.lit("20250806"))

# 写入数据
dim_platform.write.mode("append") \
    .option("compression", "snappy") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dim/dim_platform")

# 修复分区
repair_hive_table("dim_platform")

print(f"平台维度表处理完成，记录数: {dim_platform.count()}")

# ====================== DIM层支付状态维度表 ======================
print("处理支付状态维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dim/dim_payment_status")

# # 2. 删除旧表和数据
# spark.sql("DROP TABLE IF EXISTS dim_payment_status")
# delete_hdfs_path("/warehouse/gd01/dim/dim_payment_status")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL   TABLE IF not EXISTS dim_payment_status
(
    status_id     INT COMMENT '支付状态ID',
    status_name   STRING COMMENT '支付状态名称',
    status_desc   STRING COMMENT '状态描述'
) COMMENT '支付状态维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dim/dim_payment_status'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入支付状态维度表数据
print("插入支付状态维度表数据...")
# 从订单信息中提取支付状态
payment_status_from_orders = spark.sql("""
    SELECT DISTINCT is_paid
    FROM ods_product_order_log 
    WHERE dt = '20250806' AND is_paid IS NOT NULL
""")

# 创建支付状态维度数据
dim_payment_status = payment_status_from_orders.select(
    F.col("is_paid").cast("int").alias("status_id"),
    F.when(F.col("is_paid").cast("int") == 0, "已支付").otherwise("未支付").cast("string").alias("status_name"),
    F.when(F.col("is_paid").cast("int") == 0, "订单已支付").otherwise("订单未支付").cast("string").alias("status_desc")
).filter(
    F.col("is_paid").isNotNull()
).withColumn("dt", F.lit("20250806"))

# 写入数据
dim_payment_status.write.mode("append") \
    .option("compression", "snappy") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dim/dim_payment_status")

# 修复分区
repair_hive_table("dim_payment_status")

print(f"支付状态维度表处理完成，记录数: {dim_payment_status.count()}")

# ====================== DIM层类目维度表 ======================
print("处理类目维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dim/dim_category")

# # 2. 删除旧表和数据
# spark.sql("DROP TABLE IF EXISTS dim_category")
# delete_hdfs_path("/warehouse/gd01/dim/dim_category")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL   TABLE IF not EXISTS dim_category
(
    category_id     STRING COMMENT '类目ID',
    category_name   STRING COMMENT '类目名称',
    parent_id       STRING COMMENT '父类目ID',
    level           INT COMMENT '类目层级',
    is_leaf         INT COMMENT '是否叶子类目(1:是,0:否)'
) COMMENT '类目维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dim/dim_category'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入类目维度表数据
print("插入类目维度表数据...")
# 从商品信息中提取类目信息
category_from_products = spark.sql("""
    SELECT DISTINCT category_name
    FROM ods_product_info 
    WHERE dt = '20250806' AND category_name IS NOT NULL
""")

# 创建类目维度数据，使用类目名称生成类目ID
dim_category = category_from_products.select(
    F.substring(F.abs(F.hash(F.col("category_name"))).cast("string"), 1, 5).alias("category_id"),
    F.col("category_name").cast("string").alias("category_name"),
    F.lit("0").cast("string").alias("parent_id"),
    F.lit(1).cast("int").alias("level"),
    F.lit(1).cast("int").alias("is_leaf")
).filter(
    F.col("category_name").isNotNull()
).withColumn("dt", F.lit("20250806"))

# 写入数据
dim_category.write.mode("append") \
    .option("compression", "snappy") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dim/dim_category")

# 修复分区
repair_hive_table("dim_category")

print(f"类目维度表处理完成，记录数: {dim_category.count()}")

# 验证结果
print("验证DIM层数据:")
try:
    product_count = spark.sql("SELECT COUNT(*) as count FROM dim_product WHERE dt = '20250806'").collect()[0]['count']
    user_count = spark.sql("SELECT COUNT(*) as count FROM dim_user WHERE dt = '20250806'").collect()[0]['count']
    platform_count = spark.sql("SELECT COUNT(*) as count FROM dim_platform WHERE dt = '20250806'").collect()[0]['count']
    payment_status_count = spark.sql("SELECT COUNT(*) as count FROM dim_payment_status WHERE dt = '20250806'").collect()[0]['count']
    category_count = spark.sql("SELECT COUNT(*) as count FROM dim_category WHERE dt = '20250806'").collect()[0]['count']

    print(f"商品维度表记录数: {product_count}")
    print(f"用户维度表记录数: {user_count}")
    print(f"平台维度表记录数: {platform_count}")
    print(f"支付状态维度表记录数: {payment_status_count}")
    print(f"类目维度表记录数: {category_count}")

    # 展示部分数据样本
    print("商品维度表样本数据:")
    spark.sql("SELECT * FROM dim_product WHERE dt = '20250806' LIMIT 5").show(truncate=False)

    print("用户维度表样本数据:")
    spark.sql("SELECT * FROM dim_user WHERE dt = '20250806' LIMIT 5").show(truncate=False)

    print("平台维度表样本数据:")
    spark.sql("SELECT * FROM dim_platform WHERE dt = '20250806' LIMIT 5").show(truncate=False)

    print("支付状态维度表样本数据:")
    spark.sql("SELECT * FROM dim_payment_status WHERE dt = '20250806' LIMIT 5").show(truncate=False)

    print("类目维度表样本数据:")
    spark.sql("SELECT * FROM dim_category WHERE dt = '20250806' LIMIT 5").show(truncate=False)

except Exception as e:
    print(f"验证DIM层数据时出错: {e}")

print("gd01 DIM层数据处理完成!")
spark.stop()
