# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD1 ADS Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "E:/spark_temp") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.sql.parquet.binaryAsString", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 设置当前数据库
spark.sql("USE gd1")

# 设置分区日期
dt = '20250805'

print(f"开始处理GD1 ADS层数据，日期分区: {dt}")

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
    spark.sql(f"MSCK REPAIR TABLE gd1.{table_name}")
    print(f"修复分区完成：gd1.{table_name}")

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# 检查DWS层数据
print("检查DWS层数据:")
try:
    efficiency_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_efficiency WHERE dt = '{dt}'").collect()[0]['count']
    range_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_range_analysis WHERE dt = '{dt}'").collect()[0]['count']

    print(f"Efficiency表记录数: {efficiency_count}")
    print(f"Range表记录数: {range_count}")

    if efficiency_count == 0:
        print("警告: Efficiency表没有数据，请检查DWS层数据是否已正确生成")
        spark.stop()
        exit(1)

except Exception as e:
    print(f"检查DWS层数据时出错: {e}")
    spark.stop()
    exit(1)

# ====================== 商品效率监控表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd1/ads/ads_product_efficiency_monitor")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_product_efficiency_monitor")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_product_efficiency_monitor
(
    `stat_date`          STRING COMMENT '统计日期',
    `time_dimension`     STRING COMMENT '时间维度（日/月/年）',
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
    `paid_user_count`    BIGINT COMMENT '支付用户数',
    `conversion_rate`    DECIMAL(5,4) COMMENT '支付转化率'
) COMMENT '商品效率监控表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd1/ads/ads_product_efficiency_monitor'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理商品效率监控表数据...")

# 读取DWS层数据
dws_efficiency = spark.sql(f"SELECT * FROM dws_product_efficiency WHERE dt = '{dt}'")

# 聚合所有商品数据生成监控指标，确保字段类型正确
ads_product_efficiency_monitor = dws_efficiency.agg(
    F.lit("2025-08-01").cast("string").alias("stat_date"),
    F.lit("日").cast("string").alias("time_dimension"),
    F.sum("view_count").cast("long").alias("view_count"),
    F.sum("visitor_count").cast("long").alias("visitor_count"),
    F.sum("collect_count").cast("long").alias("collect_count"),
    F.sum("cart_quantity").cast("long").alias("cart_quantity"),
    F.sum("cart_user_count").cast("long").alias("cart_user_count"),
    F.sum("order_quantity").cast("long").alias("order_quantity"),
    F.sum("order_amount").cast("decimal(10,2)").alias("order_amount"),
    F.sum("order_user_count").cast("long").alias("order_user_count"),
    F.sum("paid_quantity").cast("long").alias("paid_quantity"),
    F.sum("paid_amount").cast("decimal(10,2)").alias("paid_amount"),
    F.sum("paid_user_count").cast("long").alias("paid_user_count"),
    F.when(F.sum("visitor_count") > 0,
           F.round(F.sum("paid_user_count").cast(T.DoubleType()) / F.sum("visitor_count").cast(T.DoubleType()), 4))
    .otherwise(F.lit(0.0))
    .cast("decimal(5,4)")
    .alias("conversion_rate"),
    F.lit(dt).cast("string").alias("dt")
)

# 验证数据量
print_data_count(ads_product_efficiency_monitor, "ads_product_efficiency_monitor")

# 显示部分数据以检查结果
print("ADS层商品效率监控表数据:")
ads_product_efficiency_monitor.show()

# 写入数据
ads_product_efficiency_monitor.write.mode("overwrite") \
    .option("parquet.writelegacyformat", "true") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd1/ads/ads_product_efficiency_monitor")

# 修复分区
repair_hive_table("ads_product_efficiency_monitor")

# ====================== 商品范围分析表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd1/ads/ads_product_range_analysis")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_product_range_analysis")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE ads_product_range_analysis
(
    `category_name`   STRING COMMENT '类目名称',
    `stat_date`       STRING COMMENT '统计日期',
    `time_dimension`  STRING COMMENT '时间维度（日/月/年）',
    `range_type`      STRING COMMENT '范围类型（价格/件数/金额）',
    `range_value`     STRING COMMENT '范围值',
    `product_count`   BIGINT COMMENT '商品数',
    `paid_amount`     DECIMAL(10,2) COMMENT '支付金额',
    `paid_quantity`   BIGINT COMMENT '支付件数',
    `avg_price`       DECIMAL(10,2) COMMENT '平均价格',
    `sort_metric`     STRING COMMENT '排序指标'
) COMMENT '商品范围分析表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd1/ads/ads_product_range_analysis'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理商品范围分析表数据...")

# 读取DWS层范围分析数据
dws_range_analysis = spark.sql(f"SELECT * FROM dws_product_range_analysis WHERE dt = '{dt}'")

# 处理价格范围数据
price_range_data = dws_range_analysis.select(
    F.col("category_name"),
    F.col("price_range").cast("string").alias("range_value"),
    F.col("product_count").cast("long"),
    F.col("paid_amount").cast("decimal(10,2)"),
    F.col("paid_quantity").cast("long"),
    F.col("avg_price").cast("decimal(10,2)"),
    F.lit("价格").cast("string").alias("range_type")
)

# 处理件数范围数据
quantity_range_data = dws_range_analysis.select(
    F.col("category_name"),
    F.col("quantity_range").cast("string").alias("range_value"),
    F.col("product_count").cast("long"),
    F.col("paid_amount").cast("decimal(10,2)"),
    F.col("paid_quantity").cast("long"),
    F.col("avg_price").cast("decimal(10,2)"),
    F.lit("件数").cast("string").alias("range_type")
)

# 处理金额范围数据
amount_range_data = dws_range_analysis.select(
    F.col("category_name"),
    F.col("amount_range").cast("string").alias("range_value"),
    F.col("product_count").cast("long"),
    F.col("paid_amount").cast("decimal(10,2)"),
    F.col("paid_quantity").cast("long"),
    F.col("avg_price").cast("decimal(10,2)"),
    F.lit("金额").cast("string").alias("range_type")
)

# 合并所有范围数据
combined_range_data = price_range_data.union(quantity_range_data).union(amount_range_data)

# 生成ADS层范围分析数据
ads_product_range_analysis = combined_range_data.select(
    F.col("category_name"),
    F.lit("2025-08-01").cast("string").alias("stat_date"),
    F.lit("日").cast("string").alias("time_dimension"),
    F.col("range_type"),
    F.col("range_value"),
    F.col("product_count"),
    F.col("paid_amount"),
    F.col("paid_quantity"),
    F.col("avg_price"),
    F.when(F.col("range_type") == "价格", F.col("avg_price").cast(T.StringType()))
    .when(F.col("range_type") == "件数", F.col("paid_quantity").cast(T.StringType()))
    .otherwise(F.col("paid_amount").cast(T.StringType()))
    .alias("sort_metric"),
    F.lit(dt).cast("string").alias("dt")
)

# 验证数据量
print_data_count(ads_product_range_analysis, "ads_product_range_analysis")

# 显示部分数据以检查结果
print("ADS层商品范围分析表前5条数据:")
ads_product_range_analysis.show(5)

# 写入数据
ads_product_range_analysis.write.mode("overwrite") \
    .option("parquet.writelegacyformat", "true") \
    .option("parquet.binaryAsString", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd1/ads/ads_product_range_analysis")

# 修复分区
repair_hive_table("ads_product_range_analysis")

# 验证最终结果
print("验证最终结果:")
monitor_count = spark.sql(f"SELECT COUNT(*) as count FROM ads_product_efficiency_monitor WHERE dt = '{dt}'").collect()[0]['count']
range_count = spark.sql(f"SELECT COUNT(*) as count FROM ads_product_range_analysis WHERE dt = '{dt}'").collect()[0]['count']
print(f"ADS效率监控表记录数: {monitor_count}")
print(f"ADS范围分析表记录数: {range_count}")

# 显示部分结果
print("效率监控表数据:")
spark.sql(f"SELECT * FROM ads_product_efficiency_monitor WHERE dt = '{dt}'").show()

print("范围分析表前10条数据:")
spark.sql(f"SELECT * FROM ads_product_range_analysis WHERE dt = '{dt}' LIMIT 10").show()

print("GD1 ADS层数据处理完成!")
spark.stop()
