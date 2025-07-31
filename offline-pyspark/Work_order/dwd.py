from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("DWD_Wireless_Entry_Detail") \
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

# 设置数据库
spark.sql("USE work_order")

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
    spark.sql(f"MSCK REPAIR TABLE work_order.{table_name}")
    print(f"修复分区完成：work_order.{table_name}")

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count


# ====================== 无线端入店明细DWD表 dwd_wireless_entry_detail ======================
# 创建HDFS存储目录
create_hdfs_dir("/warehouse/work_order/dwd/dwd_wireless_entry_detail")

# 【修改1】：使用CREATE IF NOT EXISTS，避免删除历史表（防止历史分区丢失）
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dwd_wireless_entry_detail (
    page_id STRING COMMENT '页面唯一标识',
    page_type STRING COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间',
    is_order TINYINT COMMENT '是否下单（1=是，0=否）',
    data_date DATE COMMENT '数据日期'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dwd/dwd_wireless_entry_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 定义处理日期
process_date = "20250709"

# 读取ODS层数据并执行清洗
dwd_data = spark.table("work_order.ods_wireless_entry").filter(
    F.col("dt") == process_date  # 按分区筛选当日数据
).select(
    "page_id",
    "page_type",
    "visitor_id",
    "visit_time",
    "is_order",
    "data_date"
).filter(
    # 清洗逻辑：过滤脏数据
    (F.col("page_id").isNotNull()) &
    (F.col("visitor_id").isNotNull()) &
    (F.col("visit_time").isNotNull()) &
    (F.col("is_order").isin(0, 1))
).dropDuplicates(["page_id", "visitor_id", "visit_time"])  # 去重

# 验证数据量
print_data_count(dwd_data, "dwd_wireless_entry_detail")

# 【修改2】：精确写入当前分区，仅覆盖当前dt的目录，不影响历史分区
# 方式：指定分区路径为 dt={process_date}，并使用overwrite模式
dwd_data.write.mode("overwrite") \
    .parquet(f"/warehouse/work_order/dwd/dwd_wireless_entry_detail/dt={process_date}")

# 修复分区（让Hive识别新写入的分区）
repair_hive_table("dwd_wireless_entry_detail")


# ====================== DWD店铺页各种页排行 ======================
# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/dwd/dwd_shop_page_visit_detail")

# 创建DWD表结构（清洗后明细）
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dwd_shop_page_visit_detail (
    shop_page_id STRING COMMENT '店铺页唯一标识',
    shop_page_subtype STRING COMMENT '店铺页细分类型（首页/活动页等）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间（标准化）',
    leave_time TIMESTAMP COMMENT '离开页面时间（标准化）',
    stay_duration INT COMMENT '停留时长（秒）',
    data_date DATE COMMENT '数据日期'
)
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dwd/dwd_shop_page_visit_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 定义处理日期
process_date = "20250701"

# 从ODS读取数据并清洗
dwd_data = spark.table("work_order.ods_shop_page_visit_detail").filter(
    F.col("dt") == process_date
).select(
    # 保留核心业务字段
    "shop_page_id",
    "shop_page_subtype",
    "visitor_id",
    # 转换为标准TIMESTAMP类型（处理可能的格式问题）
    F.to_timestamp("visit_time").alias("visit_time"),
    F.to_timestamp("leave_time").alias("leave_time"),
    "data_date"
).filter(
    # 清洗规则：过滤关键字段为空或异常的数据
    (F.col("shop_page_id").isNotNull()) &
    (F.col("visitor_id").isNotNull()) &
    (F.col("visit_time").isNotNull()) &
    (F.col("leave_time").isNotNull()) &
    # 确保离开时间晚于访问时间
    (F.col("leave_time") > F.col("visit_time"))
).withColumn(
    # 计算停留时长（秒）
    "stay_duration",
    F.unix_timestamp("leave_time") - F.unix_timestamp("visit_time")
).withColumn(
    # 补充分区字段
    "dt",
    F.lit(process_date)
)

# 验证数据
print_data_count(dwd_data, "dwd_shop_page_visit_detail")

# 写入DWD表（仅覆盖当前分区）
dwd_data.write.mode("overwrite") \
    .parquet(f"/warehouse/work_order/dwd/dwd_shop_page_visit_detail/dt={process_date}")

# 修复分区
repair_hive_table("dwd_shop_page_visit_detail")