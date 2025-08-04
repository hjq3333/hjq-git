from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ODS_Tables_With_Terminal_Type") \
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

spark.sql("USE work_order")


def create_hdfs_dir(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")


# 新增：修复Hive表分区的函数（关键）
def repair_hive_table(table_name):
    spark.sql(f"MSCK REPAIR TABLE work_order.{table_name}")
    print(f"修复分区完成：work_order.{table_name}")


# 新增：打印数据量的函数（验证数据是否存在）
def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count


# ====================== 无线端入店与承接原始数据表 ods_pc_or_wireless_entry ======================
create_hdfs_dir("/warehouse/work_order/ods/ods_pc_or_wireless_entry")
spark.sql("DROP TABLE IF EXISTS  work_order.ods_pc_or_wireless_entry")
spark.sql("""
CREATE EXTERNAL TABLE  work_order.ods_pc_or_wireless_entry (
    page_id STRING COMMENT '页面唯一标识',
    page_type STRING COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间（精确到秒）',
    is_order TINYINT COMMENT '是否下单（1=是，0=否）',
    data_date DATE COMMENT '数据日期',
    terminal_type STRING COMMENT '终端类型：pc/wireless'
) 
PARTITIONED BY (dt STRING COMMENT '数据分区日期')
STORED AS ORC
LOCATION '/warehouse/work_order/ods/ods_pc_or_wireless_entry'
TBLPROPERTIES ('orc.compress' ='snappy');
""")

# 定义CSV文件路径
csv_path = "hdfs://cdh01:8020/data/ods_wireless_entry.csv"

# 读取CSV文件，假设CSV文件有表头，字段用制表符分隔
df = spark.read.csv(
    csv_path,
    header=True,
    sep='\t',
    inferSchema=True
)

# 验证数据量
print_data_count(df, "ods_pc_or_wireless_entry")

# 添加分区列
df_with_partition = df.withColumn("dt", F.date_format(F.col("visit_time"), "yyyyMMdd"))

# 写入数据
df_with_partition.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/work_order/ods/ods_pc_or_wireless_entry")

# 修复分区
repair_hive_table("ods_pc_or_wireless_entry")


# ====================== 店内路径流转原始数据表 ods_instore_path ======================
create_hdfs_dir("/warehouse/work_order/ods/ods_instore_path")
spark.sql("DROP TABLE IF EXISTS  work_order.ods_instore_path")
spark.sql("""
CREATE EXTERNAL TABLE  work_order.ods_instore_path (
    path_record_id STRING COMMENT '路径记录唯一标识',
    visitor_id STRING COMMENT '访客唯一标识',
    source_page_id STRING COMMENT '来源页面ID',
    source_page_type STRING COMMENT '来源页面类型',
    target_page_id STRING COMMENT '去向页面ID',
    target_page_type STRING COMMENT '去向页面类型',
    jump_time TIMESTAMP COMMENT '页面跳转时间',
    visit_sequence INT COMMENT '访问顺序',
    terminal_type STRING COMMENT '终端类型：pc/wireless'
)
PARTITIONED BY (dt STRING COMMENT '数据分区日期')
STORED AS ORC
LOCATION '/warehouse/work_order/ods/ods_instore_path'
TBLPROPERTIES ('orc.compress' ='snappy');
""")

# 定义CSV文件路径
csv_path = "hdfs://cdh01:8020/data/ods_instore_path.csv"

# 读取CSV文件，假设CSV文件有表头，字段用制表符分隔
df = spark.read.csv(
    csv_path,
    header=True,
    sep='\t',
    inferSchema=True
)

# 验证数据量
print_data_count(df, "ods_instore_path")

# 添加分区列
df_with_partition = df.withColumn("dt", F.date_format(F.col("jump_time"), "yyyyMMdd"))

# 写入数据
df_with_partition.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/work_order/ods/ods_instore_path")

# 修复分区
repair_hive_table("ods_instore_path")


# ====================== 页面访问排行原始数据表 ods_page_visit_rank ======================
create_hdfs_dir("/warehouse/work_order/ods/ods_page_visit_rank")
spark.sql("DROP TABLE IF EXISTS  work_order.ods_page_visit_rank")
spark.sql("""
CREATE EXTERNAL TABLE  work_order.ods_page_visit_rank (
    page_id STRING COMMENT '页面唯一标识',
    page_type STRING COMMENT '页面类型',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间',
    leave_time TIMESTAMP COMMENT '离开页面时间',
    data_date DATE COMMENT '数据日期',
    terminal_type STRING COMMENT '终端类型：pc/wireless'
)
PARTITIONED BY (dt STRING COMMENT '数据分区日期')
STORED AS ORC
LOCATION '/warehouse/work_order/ods/ods_page_visit_rank'
TBLPROPERTIES ('orc.compress' ='snappy');
""")

# 定义CSV文件路径
csv_path = "hdfs://cdh01:8020/data/ods_page_visit_rank.csv"

# 读取CSV文件，假设CSV文件有表头，字段用制表符分隔
df = spark.read.csv(
    csv_path,
    header=True,
    sep='\t',
    inferSchema=True
)

# 验证数据量
print_data_count(df, "ods_page_visit_rank")

# 添加分区列
df_with_partition = df.withColumn("dt", F.date_format(F.col("visit_time"), "yyyyMMdd"))

# 写入数据
df_with_partition.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/work_order/ods/ods_page_visit_rank")

# 修复分区
repair_hive_table("ods_page_visit_rank")


# ====================== 店铺页细分类型访问原始数据表 ods_shop_page_visit_detail ======================
create_hdfs_dir("/warehouse/work_order/ods/ods_shop_page_visit_detail")
spark.sql("DROP TABLE IF EXISTS  work_order.ods_shop_page_visit_detail")
spark.sql("""
CREATE EXTERNAL TABLE  work_order.ods_shop_page_visit_detail (
    shop_page_id STRING COMMENT '店铺页唯一标识',
    shop_page_subtype STRING COMMENT '店铺页细分类型（首页/活动页等）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间',
    leave_time TIMESTAMP COMMENT '离开页面时间',
    data_date DATE COMMENT '数据日期',
    terminal_type STRING COMMENT '终端类型：pc/wireless'
)
PARTITIONED BY (dt STRING COMMENT '数据分区日期')
STORED AS ORC
LOCATION '/warehouse/work_order/ods/ods_shop_page_visit_detail'
TBLPROPERTIES ('orc.compress' ='snappy');
""")

# 定义CSV文件路径
csv_path = "hdfs://cdh01:8020/data/ods_shop_page_visit_detail.csv"

# 读取CSV文件，假设CSV文件有表头，字段用制表符分隔
df = spark.read.csv(
    csv_path,
    header=True,
    sep='\t',
    inferSchema=True
)


# 验证数据量
print_data_count(df, "ods_shop_page_visit_detail")

# 添加分区列
df_with_partition = df.withColumn("dt", F.date_format(F.col("visit_time"), "yyyyMMdd"))

# 写入数据
df_with_partition.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/work_order/ods/ods_shop_page_visit_detail")

# 修复分区
repair_hive_table("ods_shop_page_visit_detail")