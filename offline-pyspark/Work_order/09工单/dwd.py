from pyspark.sql import SparkSession
import datetime
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

def force_delete_hdfs_path(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if fs.exists(jvm_path):
        # 递归删除所有文件和子目录
        fs.delete(jvm_path, True)
        print(f"已强制删除HDFS路径及所有内容：{path}")
    else:
        print(f"HDFS路径不存在：{path}")


# ====================== 无线端入店明细DWD表 dwd_wireless_entry_detail ======================
create_hdfs_dir("/warehouse/work_order/dwd/dwd_wireless_entry_detail")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dwd_wireless_entry_detail (
    page_id STRING COMMENT '页面唯一标识',
    page_type STRING COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间（精确到秒）',
    is_order TINYINT COMMENT '是否下单（1=是，0=否）',
    data_date DATE COMMENT '数据日期',
    terminal_type STRING COMMENT '终端类型：wireless'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dwd/dwd_wireless_entry_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

process_date = "20250708"
# 转换日期格式为yyyy-MM-dd（修复核心错误：使用datetime类的strptime方法）
process_date_ymd = datetime.datetime.strptime(process_date, "%Y%m%d").strftime("%Y-%m-%d")

# 无线端数据处理
dwd_wireless_data = spark.table("work_order.ods_pc_or_wireless_entry").filter(
    # 修复：用变量存储转换后的日期，避免重复调用
    ((F.col("dt") == process_date) | (F.col("dt") == process_date_ymd))  # 添加括号确保逻辑正确
    & (F.lower(F.col("terminal_type")) == "wireless")
).select(
    "page_id", "page_type", "visitor_id", "visit_time", "is_order", "data_date", "terminal_type"
).filter(
    (F.col("page_id").isNotNull())
    & (F.col("visitor_id").isNotNull())
    & (F.col("visit_time").isNotNull())
    & (F.col("is_order").isin(0, 1))
).dropDuplicates(["page_id", "visitor_id", "visit_time"])

# 添加 dt 分区列
dwd_wireless_data = dwd_wireless_data.withColumn("dt", F.lit(process_date))

print(f"清洗后无线端数据量：{dwd_wireless_data.count()}")
# 使用 append 模式写入，不覆盖已有分区
dwd_wireless_data.write.mode("append") \
    .partitionBy("dt") \
    .parquet("/warehouse/work_order/dwd/dwd_wireless_entry_detail")

repair_hive_table("dwd_wireless_entry_detail")


# ====================== PC端入店明细DWD表 dwd_pc_entry_detail ======================
create_hdfs_dir("/warehouse/work_order/dwd/dwd_pc_entry_detail")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dwd_pc_entry_detail (
    page_id STRING COMMENT '页面唯一标识',
    page_type STRING COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间（精确到秒）',
    is_order TINYINT COMMENT '是否下单（1=是，0=否）',
    data_date DATE COMMENT '数据日期',
    terminal_type STRING COMMENT '终端类型：pc'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dwd/dwd_pc_entry_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 验证ODS表中是否有PC端数据（全量查询）
total_pc_in_ods = spark.table("work_order.ods_pc_or_wireless_entry").filter(
    F.lower(F.col("terminal_type")) == "pc"
).count()
print(f"ODS表中所有日期的PC端总数据量：{total_pc_in_ods}")

# 添加调试信息
if total_pc_in_ods == 0:
    # 检查ODS表中存在的所有终端类型值
    all_terminal_types = spark.table("work_order.ods_pc_or_wireless_entry") \
        .select("terminal_type").distinct().collect()
    print("ODS表中存在的所有终端类型值：")
    for row in all_terminal_types:
        print(f"  terminal_type = '{row['terminal_type']}'")

if total_pc_in_ods > 0:
    # 检查指定日期的PC端数据量
    pc_data_for_date = spark.table("work_order.ods_pc_or_wireless_entry").filter(
        ((F.col("dt") == process_date) | (F.col("dt") == process_date_ymd))
        & (F.lower(F.col("terminal_type")) == "pc")
    ).count()
    print(f"ODS表中 {process_date} 日期的PC端数据量：{pc_data_for_date}")

    if pc_data_for_date > 0:
        dwd_pc_data = spark.table("work_order.ods_pc_or_wireless_entry").filter(
            ((F.col("dt") == process_date) | (F.col("dt") == process_date_ymd))  # 添加括号确保逻辑正确
            & (F.lower(F.col("terminal_type")) == "pc")
        ).select(
            "page_id", "page_type", "visitor_id", "visit_time", "is_order", "data_date", "terminal_type"
        ).filter(
            (F.col("page_id").isNotNull())
            & (F.col("visitor_id").isNotNull())
            & (F.col("visit_time").isNotNull())
            & (F.col("is_order").isin(0, 1))
        ).dropDuplicates(["page_id", "visitor_id", "visit_time"])

        # 添加 dt 分区列
        dwd_pc_data = dwd_pc_data.withColumn("dt", F.lit(process_date))

        print(f"清洗后PC端数据量：{dwd_pc_data.count()}")
        # 使用 append 模式写入，不覆盖已有分区
        dwd_pc_data.write.mode("append") \
            .partitionBy("dt") \
            .parquet("/warehouse/work_order/dwd/dwd_pc_entry_detail")

        repair_hive_table("dwd_pc_entry_detail")
    else:
        print(f"警告：ODS表中没有 {process_date} 日期的PC端数据")
else:
    print("警告：ODS表中没有任何PC端数据，请检查数据生成逻辑！")





# ====================== DWD店铺页各种页排行 ======================
# ====================== DWD层：PC端店铺页访问明细 ======================
# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/dwd/dwd_shop_page_visit_detail_pc")

# 创建DWD表结构（清洗后明细）- PC端
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dwd_shop_page_visit_detail_pc (
    shop_page_id STRING COMMENT '店铺页唯一标识',
    shop_page_subtype STRING COMMENT '店铺页细分类型（首页/活动页等）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间（标准化）',
    leave_time TIMESTAMP COMMENT '离开页面时间（标准化）',
    stay_duration BIGINT COMMENT '停留时长（秒）',
    data_date DATE COMMENT '数据日期'
)
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dwd/dwd_shop_page_visit_detail_pc'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 定义处理日期
process_date = "20250701"

# 从ODS读取数据并清洗 - PC端
dwd_data_pc = spark.table("work_order.ods_shop_page_visit_detail").filter(
    (F.col("dt") == process_date) &
    (F.lower(F.col("terminal_type")) == "pc")
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
print_data_count(dwd_data_pc, "dwd_shop_page_visit_detail_pc")

# 写入DWD表（仅覆盖当前分区）
dwd_data_pc.write.mode("overwrite") \
    .parquet(f"/warehouse/work_order/dwd/dwd_shop_page_visit_detail_pc/dt={process_date}")

# 修复分区
repair_hive_table("dwd_shop_page_visit_detail_pc")

# ====================== DWD层：无线端店铺页访问明细 ======================
# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/dwd/dwd_shop_page_visit_detail_wireless")

# 创建DWD表结构（清洗后明细）- 无线端
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dwd_shop_page_visit_detail_wireless (
    shop_page_id STRING COMMENT '店铺页唯一标识',
    shop_page_subtype STRING COMMENT '店铺页细分类型（首页/活动页等）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间（标准化）',
    leave_time TIMESTAMP COMMENT '离开页面时间（标准化）',
    stay_duration BIGINT COMMENT '停留时长（秒）',
    data_date DATE COMMENT '数据日期'
)
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/dwd/dwd_shop_page_visit_detail_wireless'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 定义处理日期
process_date = "20250701"

# 从ODS读取数据并清洗 - 无线端
dwd_data_wireless = spark.table("work_order.ods_shop_page_visit_detail").filter(
    (F.col("dt") == process_date) &
    (F.lower(F.col("terminal_type")) == "wireless")
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
print_data_count(dwd_data_wireless, "dwd_shop_page_visit_detail_wireless")

# 写入DWD表（仅覆盖当前分区）
dwd_data_wireless.write.mode("overwrite") \
    .parquet(f"/warehouse/work_order/dwd/dwd_shop_page_visit_detail_wireless/dt={process_date}")

# 修复分区
repair_hive_table("dwd_shop_page_visit_detail_wireless")




# ====================== DWD层：PC端店内路径流转明细表 ======================
# 首次运行创建表结构（后续运行不重复创建）- PC端
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dwd_instore_path_pc (
    path_record_id STRING COMMENT '路径记录唯一标识',
    visitor_id STRING COMMENT '访客唯一标识',
    source_page_id STRING COMMENT '来源页面ID',
    source_page_type STRING COMMENT '来源页面类型',
    target_page_id STRING COMMENT '去向页面ID',
    target_page_type STRING COMMENT '去向页面类型',
    jump_time TIMESTAMP COMMENT '页面跳转时间',
    visit_sequence INT COMMENT '访问顺序',
    jump_date DATE COMMENT '跳转日期'
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/warehouse/work_order/dwd/dwd_instore_path_pc'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

create_hdfs_dir("/warehouse/work_order/dwd/dwd_instore_path_pc")

process_date = '20250702'

# 2. 从ODS层加载当日数据并清洗 - PC端
dwd_data_pc = spark.table("work_order.ods_instore_path").filter(
    (F.col("dt") == process_date) &
    (F.lower(F.col("terminal_type")) == "pc")
) \
    .withColumn("jump_time", F.to_timestamp(F.col("jump_time"))) \
    .withColumn("jump_date", F.to_date(F.col("jump_time"))) \
    .select(
    "path_record_id",
    "visitor_id",
    "source_page_id",
    "source_page_type",
    "target_page_id",
    "target_page_type",
    "jump_time",
    "visit_sequence",
    "jump_date",
    F.col("dt")
)

# 3. 追加写入当日分区（核心：不覆盖历史分区）- PC端
dwd_data_pc.write.mode("overwrite") \
    .parquet(f"/warehouse/work_order/dwd/dwd_instore_path_pc/dt={process_date}")
# 说明：这里用overwrite是只覆盖当前日期分区，不影响历史分区

repair_hive_table("dwd_instore_path_pc")

# 验证DWD数据 - PC端
print(f"DWD层PC端{process_date}新增数据量：{dwd_data_pc.count()}条")
print(f"DWD层PC端历史总数据量：{spark.table('work_order.dwd_instore_path_pc').count()}条")

# ====================== DWD层：无线端店内路径流转明细表 ======================
# 首次运行创建表结构（后续运行不重复创建）- 无线端
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.dwd_instore_path_wireless (
    path_record_id STRING COMMENT '路径记录唯一标识',
    visitor_id STRING COMMENT '访客唯一标识',
    source_page_id STRING COMMENT '来源页面ID',
    source_page_type STRING COMMENT '来源页面类型',
    target_page_id STRING COMMENT '去向页面ID',
    target_page_type STRING COMMENT '去向页面类型',
    jump_time TIMESTAMP COMMENT '页面跳转时间',
    visit_sequence INT COMMENT '访问顺序',
    jump_date DATE COMMENT '跳转日期'
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/warehouse/work_order/dwd/dwd_instore_path_wireless'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

create_hdfs_dir("/warehouse/work_order/dwd/dwd_instore_path_wireless")

process_date = '20250702'

# 2. 从ODS层加载当日数据并清洗 - 无线端
dwd_data_wireless = spark.table("work_order.ods_instore_path").filter(
    (F.col("dt") == process_date) &
    (F.lower(F.col("terminal_type")) == "wireless")
) \
    .withColumn("jump_time", F.to_timestamp(F.col("jump_time"))) \
    .withColumn("jump_date", F.to_date(F.col("jump_time"))) \
    .select(
    "path_record_id",
    "visitor_id",
    "source_page_id",
    "source_page_type",
    "target_page_id",
    "target_page_type",
    "jump_time",
    "visit_sequence",
    "jump_date",
    F.col("dt")
)

# 3. 追加写入当日分区（核心：不覆盖历史分区）- 无线端
dwd_data_wireless.write.mode("overwrite") \
    .parquet(f"/warehouse/work_order/dwd/dwd_instore_path_wireless/dt={process_date}")
# 说明：这里用overwrite是只覆盖当前日期分区，不影响历史分区

repair_hive_table("dwd_instore_path_wireless")

# 验证DWD数据 - 无线端
print(f"DWD层无线端{process_date}新增数据量：{dwd_data_wireless.count()}条")
print(f"DWD层无线端历史总数据量：{spark.table('work_order.dwd_instore_path_wireless').count()}条")



