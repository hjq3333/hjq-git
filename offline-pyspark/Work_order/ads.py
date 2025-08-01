from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import
from datetime import datetime, timedelta
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ADS_Wireless_Entry_AllPeriods") \
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

# 工具函数
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
    print(f"{table_name} 插入的数据量：{count} 行")
    return count

def delete_hdfs_path(path):
    """删除HDFS路径"""
    jvm_path = spark.sparkContext._jvm.Path(path)
    if fs.exists(jvm_path):
        fs.delete(jvm_path, True)
        print(f"已删除HDFS路径: {path}")

# 工具函数：强制删除HDFS路径（关键修改）
def force_delete_hdfs_path(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if fs.exists(jvm_path):
        # 递归删除所有文件和子目录
        fs.delete(jvm_path, True)
        print(f"已强制删除HDFS路径及所有内容：{path}")
    else:
        print(f"HDFS路径不存在：{path}")


# ====================== ADS层无线端表创建 ======================
create_hdfs_dir("/warehouse/work_order/ads/ads_wireless_entry_indicator")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.ads_wireless_entry_indicator (
    page_type STRING COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
    page_type_desc STRING COMMENT '页面类型描述',
    stat_period STRING COMMENT '统计周期（day/7day/30day）',
    end_date DATE COMMENT '周期结束日期',
    visitor_count BIGINT COMMENT '访客数',
    order_buyer_count BIGINT COMMENT '下单买家数',
    conversion_rate_pct DOUBLE COMMENT '转化率（百分比，保留2位小数）'
)
PARTITIONED BY (dt STRING COMMENT '分区日期（与end_date一致）')
STORED AS PARQUET
LOCATION '/warehouse/work_order/ads/ads_wireless_entry_indicator'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")


# ====================== ADS层PC端表创建 ======================
create_hdfs_dir("/warehouse/work_order/ads/ads_pc_entry_indicator")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.ads_pc_entry_indicator (
    page_type STRING COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
    page_type_desc STRING COMMENT '页面类型描述',
    stat_period STRING COMMENT '统计周期（day/7day/30day）',
    end_date DATE COMMENT '周期结束日期',
    visitor_count BIGINT COMMENT '访客数',
    order_buyer_count BIGINT COMMENT '下单买家数',
    conversion_rate_pct DOUBLE COMMENT '转化率（百分比，保留2位小数）'
)
PARTITIONED BY (dt STRING COMMENT '分区日期（与end_date一致）')
STORED AS PARQUET
LOCATION '/warehouse/work_order/ads/ads_pc_entry_indicator'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")


# ====================== 核心逻辑：处理无线端数据 ======================
process_date = "20250707"
end_date = datetime.strptime(process_date, "%Y%m%d").date()

# 定义店铺页包含的细分类型
shop_page_subtypes = ["首页", "活动页", "分类页", "宝贝页", "新品页"]

# 从DWS层读取无线端多周期数据
wireless_dws_data = spark.table("work_order.dws_wireless_entry_multi_period").filter(
    F.col("dt") == process_date
)

# 处理无线端数据
wireless_ads_data = wireless_dws_data.withColumn(
    # 合并页面类型：将细分类型归类为三大类
    "page_type_merged",
    F.when(
        F.col("page_type").isin(shop_page_subtypes),  # 属于店铺页的细分类型
        F.lit("店铺页")
    ).when(
        F.col("page_type") == "商品详情页",  # 商品详情页保持不变
        F.lit("商品详情页")
    ).otherwise(
        F.lit("店铺其他页")  # 其他类型归为店铺其他页
    )
).groupBy("page_type_merged", "stat_period", "end_date") \
    .agg(
    F.sum("total_visitor_count").alias("visitor_count"),
    F.sum("total_order_buyer_count").alias("order_buyer_count")
) \
    .withColumn(
    "page_type_desc",  # 补充页面类型描述
    F.when(F.col("page_type_merged") == "店铺页", "包含首页、活动页、分类页、宝贝页、新品页等")
    .when(F.col("page_type_merged") == "商品详情页", "指商品的基础详情页面")
    .when(F.col("page_type_merged") == "店铺其他页", "不属于前两类的兜底页面，如订阅页、直播页等")
    .otherwise("未知页面类型")
) \
    .withColumn(
    "conversion_rate_pct",  # 计算转化率（百分比）
    F.when(
        F.col("visitor_count") > 0,
        F.round(F.col("order_buyer_count") / F.col("visitor_count") * 100, 2)
    ).otherwise(F.lit(0.0))
) \
    .withColumn("dt", F.lit(process_date)) \
    .select(
    F.col("page_type_merged").alias("page_type"),
    "page_type_desc",
    "stat_period",
    "end_date",
    "visitor_count",
    "order_buyer_count",
    "conversion_rate_pct",
    "dt"
)

# 验证无线端数据
print_data_count(wireless_ads_data, "ads_wireless_entry_indicator")
print("\n无线端页面类型分布：")
wireless_ads_data.groupBy("page_type").count().show()

# 显示各周期的数据
print("\n无线端各周期数据分布：")
wireless_ads_data.groupBy("stat_period").count().show()

# 写入无线端ADS表
wireless_ads_data.write.mode("overwrite") \
    .option("parquet.writeLegacyFormat", "true") \
    .parquet(f"/warehouse/work_order/ads/ads_wireless_entry_indicator/dt={process_date}")

# 修复无线端表分区
repair_hive_table("ads_wireless_entry_indicator")

# ====================== 核心逻辑：处理PC端数据 ======================
# 从DWS层读取PC端多周期数据
pc_dws_data = spark.table("work_order.dws_pc_entry_multi_period").filter(
    F.col("dt") == process_date
)

# 处理PC端数据
pc_ads_data = pc_dws_data.withColumn(
    # 合并页面类型：将细分类型归类为三大类
    "page_type_merged",
    F.when(
        F.col("page_type").isin(shop_page_subtypes),  # 属于店铺页的细分类型
        F.lit("店铺页")
    ).when(
        F.col("page_type") == "商品详情页",  # 商品详情页保持不变
        F.lit("商品详情页")
    ).otherwise(
        F.lit("店铺其他页")  # 其他类型归为店铺其他页
    )
).groupBy("page_type_merged", "stat_period", "end_date") \
    .agg(
    F.sum("total_visitor_count").alias("visitor_count"),
    F.sum("total_order_buyer_count").alias("order_buyer_count")
) \
    .withColumn(
    "page_type_desc",  # 补充页面类型描述
    F.when(F.col("page_type_merged") == "店铺页", "包含首页、活动页、分类页、宝贝页、新品页等")
    .when(F.col("page_type_merged") == "商品详情页", "指商品的基础详情页面")
    .when(F.col("page_type_merged") == "店铺其他页", "不属于前两类的兜底页面，如订阅页、直播页等")
    .otherwise("未知页面类型")
) \
    .withColumn(
    "conversion_rate_pct",  # 计算转化率（百分比）
    F.when(
        F.col("visitor_count") > 0,
        F.round(F.col("order_buyer_count") / F.col("visitor_count") * 100, 2)
    ).otherwise(F.lit(0.0))
) \
    .withColumn("dt", F.lit(process_date)) \
    .select(
    F.col("page_type_merged").alias("page_type"),
    "page_type_desc",
    "stat_period",
    "end_date",
    "visitor_count",
    "order_buyer_count",
    "conversion_rate_pct",
    "dt"
)

# 验证PC端数据
print_data_count(pc_ads_data, "ads_pc_entry_indicator")
print("\nPC端页面类型分布：")
pc_ads_data.groupBy("page_type").count().show()

# 显示各周期的数据
print("\nPC端各周期数据分布：")
pc_ads_data.groupBy("stat_period").count().show()

# 写入PC端ADS表
pc_ads_data.write.mode("overwrite") \
    .option("parquet.writeLegacyFormat", "true") \
    .parquet(f"/warehouse/work_order/ads/ads_pc_entry_indicator/dt={process_date}")

# 修复PC端表分区
repair_hive_table("ads_pc_entry_indicator")

# 最终验证：读取写入的数据
try:
    print("\n无线端写入后的数据验证：")
    wireless_verify_df = spark.read.parquet(f"/warehouse/work_order/ads/ads_wireless_entry_indicator/dt={process_date}")
    wireless_verify_df.show()

    # 显示无线端各周期的转化率
    print("\n无线端各周期转化率：")
    wireless_verify_df.select("page_type", "stat_period", "visitor_count", "order_buyer_count", "conversion_rate_pct") \
        .orderBy("page_type", "stat_period").show()

    print("\nPC端写入后的数据验证：")
    pc_verify_df = spark.read.parquet(f"/warehouse/work_order/ads/ads_pc_entry_indicator/dt={process_date}")
    pc_verify_df.show()

    # 显示PC端各周期的转化率
    print("\nPC端各周期转化率：")
    pc_verify_df.select("page_type", "stat_period", "visitor_count", "order_buyer_count", "conversion_rate_pct") \
        .orderBy("page_type", "stat_period").show()
except Exception as e:
    print(f"\n数据写入后验证失败：{e}")




#
#
# # ====================== ADS层：店铺页各种页排行每日指标汇总按浏览量排行 ======================
# # 1. 删除Hive表元数据
# spark.sql("DROP TABLE IF EXISTS work_order.ads_shop_page_analysis")
# print("已删除Hive表元数据")
#
# # 2. 强制删除HDFS上的实际数据
# ads_hdfs_path = "/warehouse/work_order/ads/ads_shop_page_analysis"
# force_delete_hdfs_path(ads_hdfs_path)
#
# # 3. 重新创建空目录
# create_hdfs_dir(ads_hdfs_path)
#
# # 4. 重建表结构
# spark.sql("""
# CREATE EXTERNAL TABLE IF NOT EXISTS work_order.ads_shop_page_analysis (
#     shop_page_subtype STRING COMMENT '店铺页细分类型',
#     visitor_count BIGINT COMMENT '访客数',
#     visit_count BIGINT COMMENT '浏览量',
#     avg_stay_duration DOUBLE COMMENT '平均停留时长（秒）',
#     visit_rank INT COMMENT '当日浏览量排序（1=最高）',
#     data_date DATE COMMENT '数据日期'
# )
# PARTITIONED BY (dt STRING COMMENT '分区日期，与data_date一致')
# STORED AS PARQUET
# LOCATION '/warehouse/work_order/ads/ads_shop_page_analysis'
# TBLPROPERTIES ('parquet.compression' = 'snappy');
# """)
#
# # 5. 从DWS读取数据并处理
# dws_data = spark.table("work_order.dws_shop_page_daily_summary")
#
# # 获取DWS层总记录数（用于校验）
# dws_total_count = dws_data.count()
# print(f"DWS层总记录数：{dws_total_count}条")
#
# all_daily_ads_data = None
#
# date_list = dws_data.select("data_date").distinct().collect()
# for date_row in date_list:
#     current_date = date_row["data_date"].strftime('%Y%m%d')
#     daily_dws_data = dws_data.filter(F.col("data_date") == date_row["data_date"])
#
#     # 查看每日DWS数据量
#     daily_count = daily_dws_data.count()
#     print(f"{current_date}的DWS数据量：{daily_count}条")
#
#     # 去重（确保每日每个页面类型唯一）
#     daily_dws_data = daily_dws_data.dropDuplicates(["shop_page_subtype", "data_date"])
#
#     # 按日期分区计算排名
#     daily_ads_data = daily_dws_data.withColumn(
#         "visit_rank",
#         F.row_number().over(
#             Window.partitionBy("data_date").orderBy(F.col("visit_count").desc())
#         )
#     ).select(
#         "shop_page_subtype",
#         "visitor_count",
#         "visit_count",
#         "avg_stay_duration",
#         "visit_rank",
#         "data_date",
#         F.lit(current_date).alias("dt")
#     )
#
#     if all_daily_ads_data is None:
#         all_daily_ads_data = daily_ads_data
#     else:
#         all_daily_ads_data = all_daily_ads_data.union(daily_ads_data)
#
# # 6. 写入数据（完全匹配DWS层数据量）
# if all_daily_ads_data:
#     # 校验ADS与DWS的数据量是否一致
#     ads_total_count = all_daily_ads_data.count()
#     print(f"ADS层总记录数：{ads_total_count}条")
#
#     # 只做一致性校验，不强制固定数量
#     if ads_total_count != dws_total_count:
#         print(f"警告：ADS与DWS数据量不一致（DWS:{dws_total_count}条, ADS:{ads_total_count}条）")
#     else:
#         print("数据量校验通过：ADS与DWS记录数一致")
#
#     all_daily_ads_data.write.mode("append") \
#         .partitionBy("dt") \
#         .parquet(ads_hdfs_path)
#
# # 7. 修复分区
# repair_hive_table("ads_shop_page_analysis")
#
# # 最终验证：从Hive表读取的数量
# final_count = spark.sql("SELECT COUNT(*) FROM work_order.ads_shop_page_analysis").collect()[0][0]
# print(f"ADS表最终总记录数：{final_count}条")
#
#
#
#
# # ====================== ADS层：店内路径流转明细表路径分析结果（追加模式） ======================
# # 1. 首次运行创建表结构
# spark.sql("""
# CREATE EXTERNAL TABLE IF NOT EXISTS work_order.ads_instore_path_analysis (
#     source_page_type STRING COMMENT '来源页面类型',
#     target_page_type STRING COMMENT '去向页面类型',
#     jump_count BIGINT COMMENT '跳转次数',
#     jump_rate DOUBLE COMMENT '跳转率(相对于来源页面)',
#     path_rank INT COMMENT '按当日跳转次数排序',
#     stat_date DATE COMMENT '统计日期'
# )
# PARTITIONED BY (dt STRING)
# STORED AS PARQUET
# LOCATION '/warehouse/work_order/ads/ads_instore_path_analysis'
# TBLPROPERTIES ('parquet.compression' = 'snappy');
# """)
#
# create_hdfs_dir("/warehouse/work_order/ads/ads_instore_path_analysis")
#
# process_date = '20250701'
#
# # 2. 从DWD层计算当日页面跳转统计
# page_jumps = spark.table("work_order.dwd_instore_path").filter(F.col("dt") == process_date) \
#     .groupBy("source_page_type", "target_page_type", "jump_date") \
#     .agg(F.count("*").alias("jump_count"))
#
# # 计算当日各来源页面的总跳转次数
# source_totals = page_jumps.groupBy("source_page_type") \
#     .agg(F.sum("jump_count").alias("total_jumps_from_source"))
#
# # 关联计算跳转率并排序（排名仅针对当日数据）
# ads_data = page_jumps.join(source_totals, on="source_page_type", how="left") \
#     .withColumn("jump_rate", F.round(F.col("jump_count") / F.col("total_jumps_from_source"), 4)) \
#     .withColumn("path_rank",
#                 F.row_number().over(
#                     Window.orderBy(F.col("jump_count").desc())
#                 )) \
#     .select(
#     "source_page_type",
#     "target_page_type",
#     "jump_count",
#     "jump_rate",
#     "path_rank",
#     F.col("jump_date").alias("stat_date"),
#     F.lit(process_date).alias("dt")
# )
#
# # 3. 写入当日分区（仅覆盖当前日期，历史数据保留）
# ads_data.write.mode("overwrite") \
#     .parquet(f"/warehouse/work_order/ads/ads_instore_path_analysis/dt={process_date}")
#
# repair_hive_table("ads_instore_path_analysis")
#
# # 验证ADS数据
# print(f"ADS层{process_date}新增统计记录：{ads_data.count()}条")
# print(f"ADS层历史总记录数：{spark.table('work_order.ads_instore_path_analysis').count()}条")
#
#
# # ====================== ADS层：pcTOP20来源及占比计算（修复后） ======================
# # 1. 首次运行创建表结构（字段名同步为shop_page_subtype）
# spark.sql("""
# CREATE EXTERNAL TABLE IF NOT EXISTS work_order.ads_pc_source_top20 (
#     shop_page_subtype STRING COMMENT '店铺页细分类型',  -- 修正字段名
#     visitor_count BIGINT COMMENT '独立访客数',
#     visit_count BIGINT COMMENT '总访问次数',
#     visitor_ratio DOUBLE COMMENT '访客占比(%)',
#     visit_ratio DOUBLE COMMENT '访问次数占比(%)',
#     top_rank INT COMMENT '按访客数排名',
#     data_date DATE COMMENT '数据日期'
# )
# PARTITIONED BY (dt STRING COMMENT '分区日期')
# STORED AS PARQUET
# LOCATION '/warehouse/work_order/ads/ads_pc_source_top20'
# TBLPROPERTIES ('parquet.compression' = 'snappy');
# """)
# delete_hdfs_path("/warehouse/work_order/ads/ads_pc_source_top20")
# create_hdfs_dir("/warehouse/work_order/ads/ads_pc_source_top20")
# process_date= '20250701'
# # 2. 计算当日总指标（从DWS层获取数据）
# dws_data = spark.table("work_order.dws_shop_page_daily_summary").filter(F.col("dt") == process_date)
# total_visitors = dws_data.agg(F.sum("visitor_count")).collect()[0][0] or 0
# total_visits = dws_data.agg(F.sum("visit_count")).collect()[0][0] or 0
# print(f"当日总访客数：{total_visitors}，总访问次数：{total_visits}")
#
# # 3. 计算TOP20及占比（字段名修正为shop_page_subtype）
# ads_data = dws_data \
#     .withColumn("visitor_ratio", F.round(F.col("visitor_count") / total_visitors * 100, 2)) \
#     .withColumn("visit_ratio", F.round(F.col("visit_count") / total_visits * 100, 2)) \
#     .withColumn("top_rank", F.row_number().over(
#     Window.orderBy(F.col("visitor_count").desc())
# )) \
#     .filter(F.col("top_rank") <= 20) \
#     .select(
#     "shop_page_subtype",  # 假设正确字段名是shop_page_subtype
#     "visitor_count",
#     "visit_count",
#     "visitor_ratio",
#     "visit_ratio",
#     "top_rank",
#     "data_date",
#     F.lit(process_date).alias("dt")
# )
#
# # 4. 写入当日分区
# ads_data.write.mode("overwrite") \
#     .parquet(f"/warehouse/work_order/ads/ads_pc_source_top20/dt={process_date}")
#
# repair_hive_table("ads_pc_source_top20")
#
# # 验证结果
# print(f"\n===== {process_date} 店铺页TOP20结果 =====")
# ads_data.orderBy("top_rank").select(
#     "top_rank",
#     "shop_page_subtype",
# "visitor_count",
# F.concat(F.col("visitor_ratio"), F.lit("%")).alias("访客占比"),
# F.concat(F.col("visit_ratio"), F.lit("%")).alias("访问次数占比")
# ).show(20, truncate=False)