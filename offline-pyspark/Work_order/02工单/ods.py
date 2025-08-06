from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Ecommerce_ODS_DIM_Tables") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()

java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

# 使用指定数据库
spark.sql("USE gd02")


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
    spark.sql(f"MSCK REPAIR TABLE gd02.{table_name}")
    print(f"修复分区完成：gd02.{table_name}")


def print_data_count(df, table_name):
    """打印数据量"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# ====================== ODS层表 ======================

# 1. 商品基本信息表 ods_product_info
create_hdfs_dir("/warehouse/gd02/ods/ods_product_info")
spark.sql("DROP TABLE IF EXISTS gd02.ods_product_info")
spark.sql("""
CREATE EXTERNAL TABLE gd02.ods_product_info (
    product_id INT COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id INT COMMENT '分类ID',
    price DECIMAL(10,2) COMMENT '商品价格',
    stock_num INT COMMENT '库存数量',
    price_strength STRING COMMENT '价格力星级'
) 
PARTITIONED BY (dt STRING COMMENT '数据分区日期')
STORED AS ORC
LOCATION '/warehouse/gd02/ods/ods_product_info'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 读取CSV并写入
ods_product_csv = "hdfs://cdh01:8020/data/ods_product_info.csv"
ods_product_df = spark.read.csv(
    ods_product_csv,
    header=True,
    sep=',',
    inferSchema=True
)
print_data_count(ods_product_df, "ods_product_info")

# 确保分区字段存在
if "dt" not in ods_product_df.columns:
    ods_product_df = ods_product_df.withColumn("dt", F.col("data_date").cast("string"))

ods_product_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd02/ods/ods_product_info")
repair_hive_table("ods_product_info")


# 2. 店铺访问日志表 ods_shop_visit_log
create_hdfs_dir("/warehouse/gd02/ods/ods_shop_visit_log")
spark.sql("DROP TABLE IF EXISTS gd02.ods_shop_visit_log")
spark.sql("""
CREATE EXTERNAL TABLE gd02.ods_shop_visit_log (
    log_id STRING COMMENT '日志唯一标识',
    user_id INT COMMENT '用户ID',
    product_id INT COMMENT '商品ID',
    visit_time TIMESTAMP COMMENT '访问时间',
    visit_source STRING COMMENT '流量来源'
) 
PARTITIONED BY (dt STRING COMMENT '数据分区日期')
STORED AS ORC
LOCATION '/warehouse/gd02/ods/ods_shop_visit_log'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 读取CSV并写入
ods_visit_csv = "hdfs://cdh01:8020/data/ods_shop_visit_log.csv"
ods_visit_df = spark.read.csv(
    ods_visit_csv,
    header=True,
    sep=',',
    inferSchema=True
)
print_data_count(ods_visit_df, "ods_shop_visit_log")

# 处理分区字段
if "dt" not in ods_visit_df.columns:
    ods_visit_df = ods_visit_df.withColumn("dt", F.date_format(F.col("visit_time"), "yyyyMMdd"))

ods_visit_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd02/ods/ods_shop_visit_log")
repair_hive_table("ods_shop_visit_log")


# 3. 商品销售信息表 ods_product_sale_info
create_hdfs_dir("/warehouse/gd02/ods/ods_product_sale_info")
spark.sql("DROP TABLE IF EXISTS gd02.ods_product_sale_info")
spark.sql("""
CREATE EXTERNAL TABLE gd02.ods_product_sale_info (
    order_id STRING COMMENT '订单ID',
    product_id INT COMMENT '商品ID',
    sku_id INT COMMENT 'SKU ID',
    user_id INT COMMENT '用户ID',
    pay_amount DECIMAL(10,2) COMMENT '支付金额',
    pay_num INT COMMENT '支付件数',
    pay_time TIMESTAMP COMMENT '支付时间',
    category_id INT COMMENT '商品分类ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据分区日期')
STORED AS ORC
LOCATION '/warehouse/gd02/ods/ods_product_sale_info'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 读取CSV并写入
ods_sale_csv = "hdfs://cdh01:8020/data/ods_product_sale_info.csv"
ods_sale_df = spark.read.csv(
    ods_sale_csv,
    header=True,
    sep=',',
    inferSchema=True
)
print_data_count(ods_sale_df, "ods_product_sale_info")

# 处理分区字段
if "dt" not in ods_sale_df.columns:
    ods_sale_df = ods_sale_df.withColumn("dt", F.date_format(F.col("pay_time"), "yyyyMMdd"))

ods_sale_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd02/ods/ods_product_sale_info")
repair_hive_table("ods_product_sale_info")


# 4. 搜索日志表 ods_search_log
create_hdfs_dir("/warehouse/gd02/ods/ods_search_log")
spark.sql("DROP TABLE IF EXISTS gd02.ods_search_log")
spark.sql("""
CREATE EXTERNAL TABLE gd02.ods_search_log (
    log_id STRING COMMENT '日志唯一标识',
    user_id INT COMMENT '用户ID',
    search_word STRING COMMENT '搜索词',
    product_id INT COMMENT '搜索到的商品ID',
    search_time TIMESTAMP COMMENT '搜索时间'
) 
PARTITIONED BY (dt STRING COMMENT '数据分区日期')
STORED AS ORC
LOCATION '/warehouse/gd02/ods/ods_search_log'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 读取CSV并写入
ods_search_csv = "hdfs://cdh01:8020/data/ods_search_log.csv"
ods_search_df = spark.read.csv(
    ods_search_csv,
    header=True,
    sep=',',
    inferSchema=True
)
print_data_count(ods_search_df, "ods_search_log")

# 处理分区字段
if "dt" not in ods_search_df.columns:
    ods_search_df = ods_search_df.withColumn("dt", F.date_format(F.col("search_time"), "yyyyMMdd"))

ods_search_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd02/ods/ods_search_log")
repair_hive_table("ods_search_log")


print("所有ODS表创建及数据导入完成！")
spark.stop()
