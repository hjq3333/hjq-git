# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("gd01 DWS Layer") \
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

# 导入Java类用于HDFS操作
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

# 工具函数定义
def create_hdfs_dir(path):
    """创建HDFS目录"""
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

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# 设置当前数据库
spark.sql("USE gd01")

# ====================== DWS层商品效率汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dws/dws_product_efficiency")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dws_product_efficiency")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dws_product_efficiency (
    `product_id`         STRING COMMENT '商品ID',
    `stat_date`          STRING COMMENT '统计日期',
    `view_count`         BIGINT COMMENT '商品访问次数',
    `visitor_count`      BIGINT COMMENT '商品访客数',
    `collect_count`      BIGINT COMMENT '商品收藏人数',
    `cart_quantity`      BIGINT COMMENT '商品加购件数',
    `cart_user_count`    BIGINT COMMENT '商品加购用户数',
    `order_quantity`     BIGINT COMMENT '订单件数',
    `order_amount`       DECIMAL(10,2) COMMENT '订单金额',
    `order_user_count`   BIGINT COMMENT '下单用户数',
    `paid_quantity`      BIGINT COMMENT '支付件数',
    `paid_amount`        DECIMAL(10,2) COMMENT '支付金额',
    `paid_user_count`    BIGINT COMMENT '支付用户数'
) COMMENT '商品效率汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dws/dws_product_efficiency'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理商品效率汇总表数据...")

# 读取DWD层各事实表数据
dwd_view = spark.sql("SELECT * FROM dwd_product_view WHERE dt = '20250806'")
dwd_collect = spark.sql("SELECT * FROM dwd_product_collect WHERE dt = '20250806'")
dwd_cart = spark.sql("SELECT * FROM dwd_product_cart WHERE dt = '20250806'")
dwd_order = spark.sql("SELECT * FROM dwd_product_order WHERE dt = '20250806'")

# 按商品ID聚合各维度数据，确保字段类型正确
view_agg = dwd_view.groupBy("product_id").agg(
    F.sum("view_count").cast("long").alias("view_count"),
    F.sum("visitor_count").cast("long").alias("visitor_count")
)

collect_agg = dwd_collect.groupBy("product_id").agg(
    F.sum("collector_count").cast("long").alias("collect_count")
)

cart_agg = dwd_cart.groupBy("product_id").agg(
    F.sum("cart_quantity").cast("long").alias("cart_quantity"),
    F.sum("cart_user_count").cast("long").alias("cart_user_count")
)

order_agg = dwd_order.groupBy("product_id").agg(
    F.sum("quantity").cast("long").alias("order_quantity"),
    F.sum("amount").cast("decimal(10,2)").alias("order_amount"),
    F.sum("order_user_count").cast("long").alias("order_user_count"),
    F.sum("paid_quantity").cast("long").alias("paid_quantity"),
    F.sum("paid_amount").cast("decimal(10,2)").alias("paid_amount"),
    F.sum("paid_user_count").cast("long").alias("paid_user_count")
)

# 合并所有维度数据
dws_product_efficiency = view_agg.alias("v") \
    .join(collect_agg.alias("c"), F.col("v.product_id") == F.col("c.product_id"), "outer") \
    .join(cart_agg.alias("ca"),
          (F.col("v.product_id") == F.col("ca.product_id")) |
          (F.col("c.product_id") == F.col("ca.product_id")), "outer") \
    .join(order_agg.alias("o"),
          (F.col("v.product_id") == F.col("o.product_id")) |
          (F.col("c.product_id") == F.col("o.product_id")) |
          (F.col("ca.product_id") == F.col("o.product_id")), "outer") \
    .select(
    F.coalesce(F.col("v.product_id"), F.col("c.product_id"),
               F.col("ca.product_id"), F.col("o.product_id")).alias("product_id"),
    F.coalesce(F.col("view_count"), F.lit(0)).cast("long").alias("view_count"),
    F.coalesce(F.col("visitor_count"), F.lit(0)).cast("long").alias("visitor_count"),
    F.coalesce(F.col("collect_count"), F.lit(0)).cast("long").alias("collect_count"),
    F.coalesce(F.col("cart_quantity"), F.lit(0)).cast("long").alias("cart_quantity"),
    F.coalesce(F.col("cart_user_count"), F.lit(0)).cast("long").alias("cart_user_count"),
    F.coalesce(F.col("order_quantity"), F.lit(0)).cast("long").alias("order_quantity"),
    F.coalesce(F.col("order_amount"), F.lit(0.0)).cast("decimal(10,2)").alias("order_amount"),
    F.coalesce(F.col("order_user_count"), F.lit(0)).cast("long").alias("order_user_count"),
    F.coalesce(F.col("paid_quantity"), F.lit(0)).cast("long").alias("paid_quantity"),
    F.coalesce(F.col("paid_amount"), F.lit(0.0)).cast("decimal(10,2)").alias("paid_amount"),
    F.coalesce(F.col("paid_user_count"), F.lit(0)).cast("long").alias("paid_user_count"),
    F.lit("2025-08-01").cast("string").alias("stat_date"),
    F.lit("20250806").cast("string").alias("dt")
)

# 5. 验证数据量
print_data_count(dws_product_efficiency, "dws_product_efficiency")

# 6. 写入数据
dws_product_efficiency.write.mode("overwrite") \
    .option("parquet.writelegacyformat", "true") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dws/dws_product_efficiency")

# 7. 修复分区
repair_hive_table("dws_product_efficiency")

# ====================== DWS层商品范围分析表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd01/dws/dws_product_range_analysis")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dws_product_range_analysis")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dws_product_range_analysis (
    `category_name`     STRING COMMENT '类目名称',
    `price_range`       STRING COMMENT '价格范围',
    `quantity_range`    STRING COMMENT '支付件数范围',
    `amount_range`      STRING COMMENT '支付金额范围',
    `product_count`     BIGINT COMMENT '商品数',
    `paid_amount`       DECIMAL(10,2) COMMENT '支付金额',
    `paid_quantity`     BIGINT COMMENT '支付件数',
    `avg_price`         DECIMAL(10,2) COMMENT '平均价格'
) COMMENT '商品范围分析表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd01/dws/dws_product_range_analysis'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理商品范围分析表数据...")

# 读取商品效率表和商品维度表数据
efficiency_data = spark.sql("SELECT * FROM dws_product_efficiency WHERE dt = '20250806'")
product_dim = spark.sql("SELECT product_id, category_name, price FROM dim_product WHERE dt = '20250806'")

# 关联商品维度信息
efficiency_with_dim = efficiency_data.join(product_dim, "product_id", "left")

# 定义价格范围
price_range_expr = F.when(F.col("price") <= 50, "0-50元") \
    .when(F.col("price") <= 100, "51-100元") \
    .when(F.col("price") <= 200, "101-200元") \
    .when(F.col("price") <= 500, "201-500元") \
    .otherwise("500元以上")

# 定义支付件数范围
quantity_range_expr = F.when(F.col("paid_quantity") <= 50, "0-50件") \
    .when(F.col("paid_quantity") <= 100, "51-100件") \
    .when(F.col("paid_quantity") <= 150, "101-150件") \
    .when(F.col("paid_quantity") <= 300, "151-300件") \
    .otherwise("300件以上")

# 定义支付金额范围
amount_range_expr = F.when(F.col("paid_amount") <= 1000, "0-1000元") \
    .when(F.col("paid_amount") <= 3000, "1001-3000元") \
    .when(F.col("paid_amount") <= 5000, "3001-5000元") \
    .when(F.col("paid_amount") <= 10000, "5001-10000元") \
    .otherwise("10000元以上")

# 按类目和各范围维度聚合
dws_product_range_analysis = efficiency_with_dim.groupBy(
    "category_name",
    price_range_expr.alias("price_range"),
    quantity_range_expr.alias("quantity_range"),
    amount_range_expr.alias("amount_range")
).agg(
    F.count("*").cast("long").alias("product_count"),
    F.sum("paid_amount").cast("decimal(10,2)").alias("paid_amount"),
    F.sum("paid_quantity").cast("long").alias("paid_quantity"),
    F.when(F.sum("paid_quantity") > 0,
           F.round(F.sum("paid_amount") / F.sum("paid_quantity"), 2))
    .otherwise(F.lit(0.0))
    .cast("decimal(10,2)")
    .alias("avg_price")
).withColumn("dt", F.lit("20250806").cast("string"))

# 5. 验证数据量
print_data_count(dws_product_range_analysis, "dws_product_range_analysis")

# 6. 写入数据
dws_product_range_analysis.write.mode("overwrite") \
    .option("parquet.writelegacyformat", "true") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd01/dws/dws_product_range_analysis")

# 7. 修复分区
repair_hive_table("dws_product_range_analysis")

# 验证DWS层数据
print("验证DWS层数据:")
try:
    efficiency_count = spark.sql("SELECT COUNT(*) as count FROM dws_product_efficiency WHERE dt = '20250806'").collect()[0]['count']
    range_count = spark.sql("SELECT COUNT(*) as count FROM dws_product_range_analysis WHERE dt = '20250806'").collect()[0]['count']
    print(f"商品效率汇总表记录数: {efficiency_count}")
    print(f"商品范围分析表记录数: {range_count}")

    # 展示部分数据样本
    print("商品效率汇总表样本数据:")
    spark.sql("SELECT * FROM dws_product_efficiency WHERE dt = '20250806' LIMIT 5").show(truncate=False)

    print("商品范围分析表样本数据:")
    spark.sql("SELECT * FROM dws_product_range_analysis WHERE dt = '20250806' LIMIT 5").show(truncate=False)

except Exception as e:
    print(f"验证DWS层数据时出错: {e}")

print("gd01 DWS层数据处理完成!")
spark.stop()
