from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
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

def force_delete_hdfs_path(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if fs.exists(jvm_path):
        # 递归删除所有文件和子目录
        fs.delete(jvm_path, True)
        print(f"已强制删除HDFS路径及所有内容：{path}")
    else:
        print(f"HDFS路径不存在：{path}")

# ====================== 无线端日度汇总表 dws_wireless_entry_daily ======================
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

# 从DWD层读取无线端数据，按页面类型聚合
wireless_daily_data = spark.table("work_order.dwd_wireless_entry_detail").filter(
    F.col("dt") == process_date
).groupBy("page_type") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),  # 去重访客数
    F.countDistinct(F.when(F.col("is_order") == 1, F.col("visitor_id"))).alias("order_buyer_count")  # 下单买家数（仅统计is_order=1的访客）
) \
    .withColumn("stat_date", F.lit(stat_date)) \
    .withColumn("dt", F.lit(process_date))  # 分区字段

# 验证数据
print_data_count(wireless_daily_data, "dws_wireless_entry_daily")

# 写入无线端日度汇总表（仅覆盖当前分区）
wireless_daily_data.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_wireless_entry_daily/dt={process_date}")

# 修复分区
repair_hive_table("dws_wireless_entry_daily")


# ====================== PC端日度汇总表 dws_pc_entry_daily ======================
# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/dws/dws_pc_entry_daily")

# 创建表结构
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_pc_entry_daily (
    page_type STRING COMMENT '页面类型',
    stat_date DATE COMMENT '统计日期',
    visitor_count BIGINT COMMENT '当日访客数',
    order_buyer_count BIGINT COMMENT '当日下单买家数'
) 
PARTITIONED BY (dt STRING COMMENT '分区日期（与stat_date一致）')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_pc_entry_daily'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 从DWD层读取PC端数据，按页面类型聚合
pc_daily_data = spark.table("work_order.dwd_pc_entry_detail").filter(
    F.col("dt") == process_date
).groupBy("page_type") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),  # 去重访客数
    F.countDistinct(F.when(F.col("is_order") == 1, F.col("visitor_id"))).alias("order_buyer_count")  # 下单买家数（仅统计is_order=1的访客）
) \
    .withColumn("stat_date", F.lit(stat_date)) \
    .withColumn("dt", F.lit(process_date))  # 分区字段

# 验证数据
print_data_count(pc_daily_data, "dws_pc_entry_daily")

# 写入PC端日度汇总表（仅覆盖当前分区）
pc_daily_data.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_pc_entry_daily/dt={process_date}")

# 修复分区
repair_hive_table("dws_pc_entry_daily")


# ====================== 无线端多周期汇总表 dws_wireless_entry_multi_period ======================
# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/dws/dws_wireless_entry_multi_period")

# 创建表结构
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_wireless_entry_multi_period (
    page_type STRING COMMENT '页面类型',
    stat_period STRING COMMENT '统计周期（day/7day/30day）',
    end_date DATE COMMENT '周期结束日期',
    total_visitor_count BIGINT COMMENT '周期内总访客数',
    total_order_buyer_count BIGINT COMMENT '周期内总下单买家数',
    conversion_rate DOUBLE COMMENT '下单转化率（下单买家数/访客数）'
)
PARTITIONED BY (dt STRING COMMENT '分区日期（与end_date一致）')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_wireless_entry_multi_period'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

process_date = '20250707'

# 定义周期结束日期（与process_date一致）
end_date = datetime.strptime(process_date, "%Y%m%d").date()

# 计算7天周期的起始日期
start_date_7d = (end_date - timedelta(days=6)).strftime("%Y%m%d")

# 计算30天周期的起始日期
start_date_30d = (end_date - timedelta(days=29)).strftime("%Y%m%d")

# 1. 无线端日度数据
wireless_day_data = spark.table("work_order.dws_wireless_entry_daily").filter(
    F.col("dt") == process_date
).select(
    "page_type",
    F.lit("day").alias("stat_period"),
    F.lit(end_date).alias("end_date"),
    F.col("visitor_count").alias("total_visitor_count"),
    F.col("order_buyer_count").alias("total_order_buyer_count"),
    F.when(
        F.col("visitor_count") > 0,
        F.round(F.col("order_buyer_count") / F.col("visitor_count"), 4)
    ).otherwise(0).alias("conversion_rate"),
    F.lit(process_date).alias("dt")
)

# 2. 无线端7天数据
wireless_seven_day_data = spark.table("work_order.dws_wireless_entry_daily").filter(
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

# 3. 无线端30天数据
wireless_thirty_day_data = spark.table("work_order.dws_wireless_entry_daily").filter(
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

# 合并无线端多周期数据
wireless_multi_period_data = wireless_day_data.unionByName(wireless_seven_day_data).unionByName(wireless_thirty_day_data)

# 验证数据
print_data_count(wireless_multi_period_data, "dws_wireless_entry_multi_period")

# 写入无线端多周期汇总表（仅覆盖当前分区）
wireless_multi_period_data.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_wireless_entry_multi_period/dt={process_date}")

# 修复分区
repair_hive_table("dws_wireless_entry_multi_period")


# ====================== PC端多周期汇总表 dws_pc_entry_multi_period ======================
# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/dws/dws_pc_entry_multi_period")

# 创建表结构
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_pc_entry_multi_period (
    page_type STRING COMMENT '页面类型',
    stat_period STRING COMMENT '统计周期（day/7day/30day）',
    end_date DATE COMMENT '周期结束日期',
    total_visitor_count BIGINT COMMENT '周期内总访客数',
    total_order_buyer_count BIGINT COMMENT '周期内总下单买家数',
    conversion_rate DOUBLE COMMENT '下单转化率（下单买家数/访客数）'
)
PARTITIONED BY (dt STRING COMMENT '分区日期（与end_date一致）')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_pc_entry_multi_period'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 1. PC端日度数据
pc_day_data = spark.table("work_order.dws_pc_entry_daily").filter(
    F.col("dt") == process_date
).select(
    "page_type",
    F.lit("day").alias("stat_period"),
    F.lit(end_date).alias("end_date"),
    F.col("visitor_count").alias("total_visitor_count"),
    F.col("order_buyer_count").alias("total_order_buyer_count"),
    F.when(
        F.col("visitor_count") > 0,
        F.round(F.col("order_buyer_count") / F.col("visitor_count"), 4)
    ).otherwise(0).alias("conversion_rate"),
    F.lit(process_date).alias("dt")
)

# 2. PC端7天数据
pc_seven_day_data = spark.table("work_order.dws_pc_entry_daily").filter(
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

# 3. PC端30天数据
pc_thirty_day_data = spark.table("work_order.dws_pc_entry_daily").filter(
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

# 合并PC端多周期数据
pc_multi_period_data = pc_day_data.unionByName(pc_seven_day_data).unionByName(pc_thirty_day_data)

# 验证数据
print_data_count(pc_multi_period_data, "dws_pc_entry_multi_period")

# 写入PC端多周期汇总表（仅覆盖当前分区）
pc_multi_period_data.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_pc_entry_multi_period/dt={process_date}")

# 修复分区
repair_hive_table("dws_pc_entry_multi_period")




# ====================== DWS层：PC端店铺页各种页排行每日指标汇总 ======================
spark.sql("""
DROP TABLE IF EXISTS work_order.dws_shop_page_daily_summary_pc
""")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_shop_page_daily_summary_pc (
    shop_page_id STRING COMMENT '店铺页唯一标识',
    shop_page_subtype STRING COMMENT '店铺页细分类型（首页/活动页等）',
    data_date DATE COMMENT '数据日期',
    visitor_count BIGINT COMMENT '访客数',
    visit_count BIGINT COMMENT '浏览量（访问次数）',
    avg_stay_duration DOUBLE COMMENT '平均停留时长（秒）'
)
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_shop_page_daily_summary_pc'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

process_date = '20250701'

# 从 DWD 层计算 DWS 指标，包含 shop_page_id 字段 - PC 端
dws_data_pc = spark.table("work_order.dwd_shop_page_visit_detail_pc").filter(
    F.col("dt") == process_date
).groupBy("shop_page_id", "shop_page_subtype", "data_date") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),  # 访客数
    F.count("*").alias("visit_count"),  # 浏览量（访问次数）
    # 计算平均停留时长
    F.avg(F.col("stay_duration").cast(T.DoubleType())).alias("avg_stay_duration")
).withColumn("dt", F.lit(process_date))

# 写入 DWS 表 - PC 端
dws_data_pc.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_shop_page_daily_summary_pc/dt={process_date}")

repair_hive_table("dws_shop_page_daily_summary_pc")

# 验证 DWS 数据 - PC 端
print("DWS 层 PC 端数据预览：")
dws_data_pc.show()
print("DWS 层 PC 端字段类型：")
dws_data_pc.printSchema()


# ====================== DWS层：无线端店铺页各种页排行每日指标汇总 ======================
# 修改 DWS 层无线端店铺页汇总表结构，添加 shop_page_id 字段
spark.sql("""
DROP TABLE IF EXISTS work_order.dws_shop_page_daily_summary_wireless
""")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_shop_page_daily_summary_wireless (
    shop_page_id STRING COMMENT '店铺页唯一标识',
    shop_page_subtype STRING COMMENT '店铺页细分类型（首页/活动页等）',
    data_date DATE COMMENT '数据日期',
    visitor_count BIGINT COMMENT '访客数',
    visit_count BIGINT COMMENT '浏览量（访问次数）',
    avg_stay_duration DOUBLE COMMENT '平均停留时长（秒）'
)
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_shop_page_daily_summary_wireless'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

process_date = '20250701'

# 从 DWD 层计算 DWS 指标，包含 shop_page_id 字段 - 无线端
dws_data_wireless = spark.table("work_order.dwd_shop_page_visit_detail_wireless").filter(
    F.col("dt") == process_date
).groupBy("shop_page_id", "shop_page_subtype", "data_date") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),  # 访客数
    F.count("*").alias("visit_count"),  # 浏览量（访问次数）
    # 计算平均停留时长
    F.avg(F.col("stay_duration").cast(T.DoubleType())).alias("avg_stay_duration")
).withColumn("dt", F.lit(process_date))

# 写入 DWS 表 - 无线端
dws_data_wireless.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_shop_page_daily_summary_wireless/dt={process_date}")

repair_hive_table("dws_shop_page_daily_summary_wireless")

# 验证 DWS 数据 - 无线端
print("DWS 层无线端数据预览：")
dws_data_wireless.show()
print("DWS 层无线端字段类型：")
dws_data_wireless.printSchema()




# ====================== DWS层：PC端路径聚合 ======================
# 1. 首次运行创建表结构 - PC端
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_instore_path_summary_pc (
    visitor_id STRING COMMENT '访客唯一标识',
    path_sequence ARRAY<STRUCT<
        page_id: STRING,
        page_type: STRING,
        visit_time: TIMESTAMP,
        sequence: INT
    >> COMMENT '页面访问序列',
    full_path STRING COMMENT '完整路径字符串，用->分隔',
    path_length INT COMMENT '路径长度（页面跳转次数）',
    first_page_id STRING COMMENT '首个访问页面ID',
    first_page_type STRING COMMENT '首个访问页面类型',
    last_page_id STRING COMMENT '最后访问页面ID',
    last_page_type STRING COMMENT '最后访问页面类型',
    visit_date DATE COMMENT '访问日期'
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_instore_path_summary_pc'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

create_hdfs_dir("/warehouse/work_order/dws/dws_instore_path_summary_pc")

process_date = '20250702'

# 2. 从DWD层加载当日数据聚合 - PC端
source_pages_pc = spark.table("work_order.dwd_instore_path_pc").filter(F.col("dt") == process_date) \
    .select(
    F.col("visitor_id"),
    F.col("source_page_id").alias("page_id"),
    F.col("source_page_type").alias("page_type"),
    F.col("jump_time").alias("visit_time"),
    F.col("visit_sequence"),
    F.col("jump_date")
)

target_pages_pc = spark.table("work_order.dwd_instore_path_pc").filter(F.col("dt") == process_date) \
    .select(
    F.col("visitor_id"),
    F.col("target_page_id").alias("page_id"),
    F.col("target_page_type").alias("page_type"),
    F.col("jump_time").alias("visit_time"),
    F.col("visit_sequence"),
    F.col("jump_date")
)

# 合并并去重当日页面访问记录 - PC端
all_pages_pc = source_pages_pc.unionByName(target_pages_pc).dropDuplicates(
    ["visitor_id", "page_id", "visit_sequence"]
)

# 按访客ID聚合当日路径 - PC端
dws_data_pc = all_pages_pc.groupBy("visitor_id", "jump_date") \
    .agg(
    F.collect_list(
        F.struct(
            "page_id",
            "page_type",
            "visit_time",
            "visit_sequence"
        )
    ).alias("path_sequence_raw")
) \
    .withColumn("path_sequence_sorted",
                F.sort_array("path_sequence_raw", asc=True)) \
    .withColumn("full_path",
                F.array_join(
                    F.transform("path_sequence_sorted",
                                lambda x: x.page_type),
                    "->"
                )) \
    .withColumn("path_length",
                F.size("path_sequence_sorted") - 1) \
    .withColumn("first_page",
                F.element_at("path_sequence_sorted", 1)) \
    .withColumn("last_page",
                F.element_at("path_sequence_sorted", -1)) \
    .select(
    "visitor_id",
    F.col("path_sequence_sorted").alias("path_sequence"),
    "full_path",
    "path_length",
    F.col("first_page.page_id").alias("first_page_id"),
    F.col("first_page.page_type").alias("first_page_type"),
    F.col("last_page.page_id").alias("last_page_id"),
    F.col("last_page.page_type").alias("last_page_type"),
    F.col("jump_date").alias("visit_date"),
    F.lit(process_date).alias("dt")
)

# 3. 写入当日分区（仅覆盖当前日期，保留历史）- PC端
dws_data_pc.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_instore_path_summary_pc/dt={process_date}")

repair_hive_table("dws_instore_path_summary_pc")

# 验证DWS数据 - PC端
print(f"DWS层PC端{process_date}新增路径数：{dws_data_pc.count()}条")
print(f"DWS层PC端历史总路径数：{spark.table('work_order.dws_instore_path_summary_pc').count()}条")

# ====================== DWS层：无线端路径聚合 ======================
# 1. 首次运行创建表结构 - 无线端
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_instore_path_summary_wireless (
    visitor_id STRING COMMENT '访客唯一标识',
    path_sequence ARRAY<STRUCT<
        page_id: STRING,
        page_type: STRING,
        visit_time: TIMESTAMP,
        sequence: INT
    >> COMMENT '页面访问序列',
    full_path STRING COMMENT '完整路径字符串，用->分隔',
    path_length INT COMMENT '路径长度（页面跳转次数）',
    first_page_id STRING COMMENT '首个访问页面ID',
    first_page_type STRING COMMENT '首个访问页面类型',
    last_page_id STRING COMMENT '最后访问页面ID',
    last_page_type STRING COMMENT '最后访问页面类型',
    visit_date DATE COMMENT '访问日期'
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_instore_path_summary_wireless'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

create_hdfs_dir("/warehouse/work_order/dws/dws_instore_path_summary_wireless")

process_date = '20250702'

# 2. 从DWD层加载当日数据聚合 - 无线端
source_pages_wireless = spark.table("work_order.dwd_instore_path_wireless").filter(F.col("dt") == process_date) \
    .select(
    F.col("visitor_id"),
    F.col("source_page_id").alias("page_id"),
    F.col("source_page_type").alias("page_type"),
    F.col("jump_time").alias("visit_time"),
    F.col("visit_sequence"),
    F.col("jump_date")
)

target_pages_wireless = spark.table("work_order.dwd_instore_path_wireless").filter(F.col("dt") == process_date) \
    .select(
    F.col("visitor_id"),
    F.col("target_page_id").alias("page_id"),
    F.col("target_page_type").alias("page_type"),
    F.col("jump_time").alias("visit_time"),
    F.col("visit_sequence"),
    F.col("jump_date")
)

# 合并并去重当日页面访问记录 - 无线端
all_pages_wireless = source_pages_wireless.unionByName(target_pages_wireless).dropDuplicates(
    ["visitor_id", "page_id", "visit_sequence"]
)

# 按访客ID聚合当日路径 - 无线端
dws_data_wireless = all_pages_wireless.groupBy("visitor_id", "jump_date") \
    .agg(
    F.collect_list(
        F.struct(
            "page_id",
            "page_type",
            "visit_time",
            "visit_sequence"
        )
    ).alias("path_sequence_raw")
) \
    .withColumn("path_sequence_sorted",
                F.sort_array("path_sequence_raw", asc=True)) \
    .withColumn("full_path",
                F.array_join(
                    F.transform("path_sequence_sorted",
                                lambda x: x.page_type),
                    "->"
                )) \
    .withColumn("path_length",
                F.size("path_sequence_sorted") - 1) \
    .withColumn("first_page",
                F.element_at("path_sequence_sorted", 1)) \
    .withColumn("last_page",
                F.element_at("path_sequence_sorted", -1)) \
    .select(
    "visitor_id",
    F.col("path_sequence_sorted").alias("path_sequence"),
    "full_path",
    "path_length",
    F.col("first_page.page_id").alias("first_page_id"),
    F.col("first_page.page_type").alias("first_page_type"),
    F.col("last_page.page_id").alias("last_page_id"),
    F.col("last_page.page_type").alias("last_page_type"),
    F.col("jump_date").alias("visit_date"),
    F.lit(process_date).alias("dt")
)

# 3. 写入当日分区（仅覆盖当前日期，保留历史）- 无线端
dws_data_wireless.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_instore_path_summary_wireless/dt={process_date}")

repair_hive_table("dws_instore_path_summary_wireless")

# 验证DWS数据 - 无线端
print(f"DWS层无线端{process_date}新增路径数：{dws_data_wireless.count()}条")
print(f"DWS层无线端历史总路径数：{spark.table('work_order.dws_instore_path_summary_wireless').count()}条")


# ====================== DWS层：PC端页面类型来源分析 ======================
# 1. 创建表结构
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dws_pc_entry_page_type_analysis (
    source_page_type STRING COMMENT '来源页面类型',
    visitor_count BIGINT COMMENT '独立访客数',
    visit_count BIGINT COMMENT '总访问次数',
    data_date DATE COMMENT '数据日期'
)
PARTITIONED BY (dt STRING COMMENT '分区日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dws/dws_pc_entry_page_type_analysis'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

create_hdfs_dir("/warehouse/work_order/dws/dws_pc_entry_page_type_analysis")

process_date = '20250701'

# 2. 从DWD层PC端入店明细表按页面类型聚合数据
dws_data = spark.table("work_order.dwd_pc_entry_detail") \
    .filter(F.col("dt") == process_date) \
    .groupBy("page_type", "data_date") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),
    F.count("*").alias("visit_count")
) \
    .withColumn("source_page_type", F.col("page_type")) \
    .withColumn("dt", F.lit(process_date)) \
    .select(
    "source_page_type",
    "visitor_count",
    "visit_count",
    "data_date",
    "dt"
)

# 3. 写入当日分区
dws_data.write.mode("append") \
    .parquet(f"/warehouse/work_order/dws/dws_pc_entry_page_type_analysis/dt={process_date}")

repair_hive_table("dws_pc_entry_page_type_analysis")
print(f"DWS层PC端页面类型分析{process_date}处理完成，数据量：{dws_data.count()}条")

