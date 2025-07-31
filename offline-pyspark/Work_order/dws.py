from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from py4j.java_gateway import java_import
from datetime import datetime,timedelta  # 添加datetime导入

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


# ====================== 日度汇总表 dws_wireless_entry_daily ======================
# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/dws/dws_wireless_entry_daily")

# 创建表结构
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_wireless_entry_daily (
    page_type STRING COMMENT '页面类型',
    stat_date DATE COMMENT '统计日期',
    visitor_count BIGINT COMMENT '当日访客数',
    order_buyer_count BIGINT COMMENT '当日下单买家数'
) 
PARTITIONED BY (dt STRING COMMENT '分区日期（与stat_date一致）')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_wireless_entry_daily'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 定义处理日期（例如20250702）
process_date = "20250708"
stat_date = datetime.strptime(process_date, "%Y%m%d").date()  # 转换为DATE类型

# 从DWD层读取数据，按页面类型聚合
daily_data = spark.table("work_order.dwd_wireless_entry_detail").filter(
    F.col("dt") == process_date
).groupBy("page_type") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),  # 去重访客数
    F.countDistinct(F.when(F.col("is_order") == 1, F.col("visitor_id"))).alias("order_buyer_count")  # 下单买家数（仅统计is_order=1的访客）
) \
    .withColumn("stat_date", F.lit(stat_date)) \
    .withColumn("dt", F.lit(process_date))  # 分区字段


# 验证数据
print_data_count(daily_data, "dws_wireless_entry_daily")

# 写入日度汇总表（仅覆盖当前分区）
daily_data.write.mode("overwrite") \
    .parquet(f"/warehouse/work_order/dws/dws_wireless_entry_daily/dt={process_date}")

# 修复分区
repair_hive_table("dws_wireless_entry_daily")


# ====================== 多周期汇总表 dws_wireless_entry_multi_period ======================
# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/dws/dws_wireless_entry_multi_period")

# 在Spark代码中执行
spark.sql("DROP TABLE IF EXISTS work_order.dws_wireless_entry_multi_period")
# 创建表结构（修正注释格式）
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_wireless_entry_multi_period (
    page_type STRING COMMENT '页面类型',
    stat_period STRING COMMENT '统计周期（day/7day/30day）',
    end_date DATE COMMENT '周期结束日期',  -- 例如7天周期的结束日期为2025-07-02
    total_visitor_count BIGINT COMMENT '周期内总访客数',
    total_order_buyer_count BIGINT COMMENT '周期内总下单买家数',
    conversion_rate DOUBLE COMMENT '下单转化率（下单买家数/访客数）'
) 
PARTITIONED BY (dt STRING COMMENT '分区日期（与end_date一致）')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_wireless_entry_multi_period'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 定义周期结束日期（与process_date一致）
# 定义处理日期（字符串格式）
process_date = "20250707"

# 关键：将字符串转换为 date 类型（先转 datetime，再取 date 部分）
end_date = datetime.strptime(process_date, "%Y%m%d").date()

# 计算7天周期的起始日期（关键：定义start_date_7d）
start_date_7d = (end_date - timedelta(days=6)).strftime("%Y%m%d")  # 前6天到当天共7天

# 计算30天周期的起始日期（如果需要）
start_date_30d = (end_date - timedelta(days=29)).strftime("%Y%m%d")

# 1. 日度数据（修改字段名与多周期表一致）
day_data = spark.table("work_order.dws_wireless_entry_daily").filter(
    F.col("dt") == process_date
).select(
    "page_type",
    F.lit("day").alias("stat_period"),
    F.lit(end_date).alias("end_date"),
    # 关键：将单日的 visitor_count 重命名为 total_visitor_count
    F.col("visitor_count").alias("total_visitor_count"),
    # 关键：将单日的 order_buyer_count 重命名为 total_order_buyer_count
    F.col("order_buyer_count").alias("total_order_buyer_count"),
    # 转化率字段名保持一致
    F.when(
        F.col("visitor_count") > 0,
        F.round(F.col("order_buyer_count") / F.col("visitor_count"), 4)
    ).otherwise(0).alias("conversion_rate"),
    F.lit(process_date).alias("dt")
)

# 2. 7天数据（字段名不变，已为 total_xxx）
seven_day_data = spark.table("work_order.dws_wireless_entry_daily").filter(
    F.col("dt").between(start_date_7d, process_date)
).groupBy("page_type") \
    .agg(
    F.sum("visitor_count").alias("total_visitor_count"),
    F.sum("order_buyer_count").alias("total_order_buyer_count")
) \
    .select(
    "page_type",
    F.lit("7day").alias("stat_period"),
    F.lit(end_date).alias("end_date"),
    "total_visitor_count",
    "total_order_buyer_count",
    F.when(
        F.col("total_visitor_count") > 0,
        F.round(F.col("total_order_buyer_count") / F.col("total_visitor_count"), 4)
    ).otherwise(0).alias("conversion_rate"),
    F.lit(process_date).alias("dt")
)

# 3. 30天数据（字段名不变，已为 total_xxx）
thirty_day_data = spark.table("work_order.dws_wireless_entry_daily").filter(
    F.col("dt").between(start_date_30d, process_date)
).groupBy("page_type") \
    .agg(
    F.sum("visitor_count").alias("total_visitor_count"),
    F.sum("order_buyer_count").alias("total_order_buyer_count")
) \
    .select(
    "page_type",
    F.lit("30day").alias("stat_period"),
    F.lit(end_date).alias("end_date"),
    "total_visitor_count",
    "total_order_buyer_count",
    F.when(
        F.col("total_visitor_count") > 0,
        F.round(F.col("total_order_buyer_count") / F.col("total_visitor_count"), 4)
    ).otherwise(0).alias("conversion_rate"),
    F.lit(process_date).alias("dt")
)

# 此时三个 DataFrame 字段名完全一致，可以合并
multi_period_data = day_data.unionByName(seven_day_data).unionByName(thirty_day_data)

# 验证数据
print_data_count(multi_period_data, "dws_wireless_entry_multi_period")

# 写入多周期汇总表（仅覆盖当前分区）
multi_period_data.write.mode("overwrite") \
    .parquet(f"/warehouse/work_order/dws/dws_wireless_entry_multi_period/dt={process_date}")

# 修复分区
repair_hive_table("dws_wireless_entry_multi_period")

