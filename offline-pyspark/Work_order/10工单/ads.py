# -*- coding: utf-8 -*-
# 工单编号：大数据-电商数仓-10-流量主题页面分析看板

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD10 ADS Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "D:/spark_temp") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 设置当前数据库
spark.sql("USE gd10")

# 设置分区日期
dt = '20250804'

print(f"开始处理GD10 ADS层数据，日期分区: {dt}")

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
    spark.sql(f"MSCK REPAIR TABLE gd10.{table_name}")
    print(f"修复分区完成：gd10.{table_name}")

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# 检查DWS层数据
print("检查DWS层数据:")
try:
    traffic_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_page_traffic_analysis_d WHERE dt = '{dt}'").collect()[0]['count']
    click_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_page_click_distribution_d WHERE dt = '{dt}'").collect()[0]['count']
    guidance_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_page_guidance_effect_d WHERE dt = '{dt}'").collect()[0]['count']

    print(f"Traffic表记录数: {traffic_count}")
    print(f"Click表记录数: {click_count}")
    print(f"Guidance表记录数: {guidance_count}")

    if traffic_count == 0:
        print("警告: Traffic表没有数据，请检查DWS层数据是否已正确生成")
        spark.stop()
        exit(1)

except Exception as e:
    print(f"检查DWS层数据时出错: {e}")
    spark.stop()
    exit(1)

# ====================== 页面流量分析看板 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/ads/ads_page_traffic_dashboard")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_page_traffic_dashboard")

# 3. 创建外部表 - 修改时间戳类型为字符串以避免兼容性问题
spark.sql("""
CREATE EXTERNAL TABLE ads_page_traffic_dashboard
(
    `store_id`             STRING COMMENT '店铺ID',
    `page_type`            STRING COMMENT '页面类型',
    `page_views`           BIGINT COMMENT '页面浏览量',
    `unique_visitors`      BIGINT COMMENT '独立访客数',
    `avg_stay_duration`    DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    `bounce_rate`          DECIMAL(5,2) COMMENT '跳出率(%)',
    `search_count`         BIGINT COMMENT '搜索次数',
    `avg_search_rank`      DECIMAL(5,2) COMMENT '平均搜索排名',
    `traffic_score`        DECIMAL(5,2) COMMENT '流量得分(0-100)',
    `update_time`          STRING COMMENT '更新时间'
) COMMENT '页面流量分析看板'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd10/ads/ads_page_traffic_dashboard'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理页面流量分析看板数据...")

# 读取DWS层数据
dws_traffic = spark.sql(f"SELECT * FROM dws_page_traffic_analysis_d WHERE dt = '{dt}'")

# 计算流量得分 - 更加稳健的处理方式
ads_traffic_dashboard = dws_traffic.select(
    F.coalesce(F.col("store_id"), F.lit("")).cast(T.StringType()).alias("store_id"),
    F.coalesce(F.col("page_type"), F.lit("")).cast(T.StringType()).alias("page_type"),
    F.coalesce(F.col("page_views"), F.lit(0)).cast(T.LongType()).alias("page_views"),
    F.coalesce(F.col("unique_visitors"), F.lit(0)).cast(T.LongType()).alias("unique_visitors"),
    F.coalesce(F.col("avg_stay_duration"), F.lit(0)).cast(T.DecimalType(10,2)).alias("avg_stay_duration"),
    F.coalesce(F.col("bounce_rate"), F.lit(0)).cast(T.DecimalType(5,2)).alias("bounce_rate"),
    F.coalesce(F.col("search_count"), F.lit(0)).cast(T.LongType()).alias("search_count"),
    F.coalesce(F.col("avg_search_rank"), F.lit(0)).cast(T.DecimalType(5,2)).alias("avg_search_rank"),
    # 流量得分计算：基于浏览量、访客数、停留时长、跳出率等综合评估
    F.coalesce(
        F.round(
            F.when(
                (F.coalesce(F.col("page_views"), F.lit(0)) > 0) | (F.coalesce(F.col("unique_visitors"), F.lit(0)) > 0),
                F.least(
                    (F.coalesce(F.col("page_views"), F.lit(0)).cast(T.DoubleType()) / 1000 * 30 +
                     F.coalesce(F.col("unique_visitors"), F.lit(0)).cast(T.DoubleType()) / 100 * 30 +
                     F.coalesce(F.col("avg_stay_duration"), F.lit(0)).cast(T.DoubleType()) / 10 * 20 +
                     (100 - F.coalesce(F.col("bounce_rate"), F.lit(0)).cast(T.DoubleType())) * 20) / 100 * 100,
                    F.lit(100)
                )
            ).otherwise(F.lit(0)),
            2
        ).cast(T.DecimalType(5,2)),
        F.lit(0).cast(T.DecimalType(5,2))
    ).alias("traffic_score"),
    F.current_timestamp().cast(T.StringType()).alias("update_time"),
    F.lit(dt).cast(T.StringType()).alias("dt")
)

# 验证数据量
print_data_count(ads_traffic_dashboard, "ads_page_traffic_dashboard")

# 显示部分数据以检查结果
print("ADS层页面流量分析看板前10条数据:")
ads_traffic_dashboard.show(10)

# 写入数据
ads_traffic_dashboard.write.mode("overwrite") \
    .option("compression", "snappy") \
    .option("parquet.writeLegacyFormat", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd10/ads/ads_page_traffic_dashboard")

# 修复分区
repair_hive_table("ads_page_traffic_dashboard")

# ====================== 页面点击分布看板 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/ads/ads_page_click_dashboard")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_page_click_dashboard")

# 3. 创建外部表 - 修改时间戳类型为字符串以避免兼容性问题
spark.sql("""
CREATE EXTERNAL TABLE ads_page_click_dashboard
(
    `store_id`             STRING COMMENT '店铺ID',
    `page_type`            STRING COMMENT '页面类型',
    `click_count`          BIGINT COMMENT '点击次数',
    `click_users`          BIGINT COMMENT '点击用户数',
    `conversion_clicks`    BIGINT COMMENT '转化点击数(加购/支付)',
    `conversion_rate`      DECIMAL(5,2) COMMENT '转化率(%)',
    `click_score`          DECIMAL(5,2) COMMENT '点击得分(0-100)',
    `update_time`          STRING COMMENT '更新时间'
) COMMENT '页面点击分布看板'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd10/ads/ads_page_click_dashboard'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理页面点击分布看板数据...")

# 读取DWS层数据
dws_click = spark.sql(f"SELECT * FROM dws_page_click_distribution_d WHERE dt = '{dt}'")

# 计算点击得分 - 更加稳健的处理方式
ads_click_dashboard = dws_click.select(
    F.coalesce(F.col("store_id"), F.lit("")).cast(T.StringType()).alias("store_id"),
    F.coalesce(F.col("page_type"), F.lit("")).cast(T.StringType()).alias("page_type"),
    F.coalesce(F.col("click_count"), F.lit(0)).cast(T.LongType()).alias("click_count"),
    F.coalesce(F.col("click_users"), F.lit(0)).cast(T.LongType()).alias("click_users"),
    F.coalesce(F.col("conversion_clicks"), F.lit(0)).cast(T.LongType()).alias("conversion_clicks"),
    F.coalesce(F.col("conversion_rate"), F.lit(0)).cast(T.DecimalType(5,2)).alias("conversion_rate"),
    # 点击得分计算：基于点击次数、点击用户数、转化点击数综合评估
    F.coalesce(
        F.round(
            F.when(
                (F.coalesce(F.col("click_count"), F.lit(0)) > 0) | (F.coalesce(F.col("click_users"), F.lit(0)) > 0),
                F.least(
                    (F.coalesce(F.col("click_count"), F.lit(0)).cast(T.DoubleType()) / 100 * 40 +
                     F.coalesce(F.col("click_users"), F.lit(0)).cast(T.DoubleType()) / 50 * 30 +
                     F.coalesce(F.col("conversion_clicks"), F.lit(0)).cast(T.DoubleType()) / 10 * 30) / 100 * 100,
                    F.lit(100)
                )
            ).otherwise(F.lit(0)),
            2
        ).cast(T.DecimalType(5,2)),
        F.lit(0).cast(T.DecimalType(5,2))
    ).alias("click_score"),
    F.current_timestamp().cast(T.StringType()).alias("update_time"),
    F.lit(dt).cast(T.StringType()).alias("dt")
)

# 验证数据量
print_data_count(ads_click_dashboard, "ads_page_click_dashboard")

# 写入数据
ads_click_dashboard.write.mode("overwrite") \
    .option("compression", "snappy") \
    .option("parquet.writeLegacyFormat", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd10/ads/ads_page_click_dashboard")

# 修复分区
repair_hive_table("ads_page_click_dashboard")

# ====================== 页面引导效果看板 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/ads/ads_page_guidance_dashboard")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_page_guidance_dashboard")

# 3. 创建外部表 - 修改时间戳类型为字符串以避免兼容性问题
spark.sql("""
CREATE EXTERNAL TABLE ads_page_guidance_dashboard
(
    `store_id`                 STRING COMMENT '店铺ID',
    `page_type`                STRING COMMENT '页面类型',
    `page_views`               BIGINT COMMENT '页面浏览量',
    `to_item_detail_views`     BIGINT COMMENT '引导至商品详情页浏览量',
    `guidance_rate`            DECIMAL(5,2) COMMENT '引导率(%)',
    `payment_users`            BIGINT COMMENT '引导支付用户数',
    `payment_amount`           DECIMAL(15,2) COMMENT '引导支付金额',
    `guidance_score`           DECIMAL(5,2) COMMENT '引导得分(0-100)',
    `update_time`              STRING COMMENT '更新时间'
) COMMENT '页面引导效果看板'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd10/ads/ads_page_guidance_dashboard'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理页面引导效果看板数据...")

# 读取DWS层数据
dws_guidance = spark.sql(f"SELECT * FROM dws_page_guidance_effect_d WHERE dt = '{dt}'")

# 计算引导得分 - 更加稳健的处理方式
ads_guidance_dashboard = dws_guidance.select(
    F.coalesce(F.col("store_id"), F.lit("")).cast(T.StringType()).alias("store_id"),
    F.coalesce(F.col("page_type"), F.lit("")).cast(T.StringType()).alias("page_type"),
    F.coalesce(F.col("page_views"), F.lit(0)).cast(T.LongType()).alias("page_views"),
    F.coalesce(F.col("to_item_detail_views"), F.lit(0)).cast(T.LongType()).alias("to_item_detail_views"),
    F.coalesce(F.col("guidance_rate"), F.lit(0)).cast(T.DecimalType(5,2)).alias("guidance_rate"),
    F.coalesce(F.col("payment_users"), F.lit(0)).cast(T.LongType()).alias("payment_users"),
    F.coalesce(F.col("payment_amount"), F.lit(0)).cast(T.DecimalType(15,2)).alias("payment_amount"),
    # 引导得分计算：基于引导率、支付用户数、支付金额综合评估
    F.coalesce(
        F.round(
            F.when(
                (F.coalesce(F.col("guidance_rate"), F.lit(0)) > 0) | (F.coalesce(F.col("payment_users"), F.lit(0)) > 0),
                F.least(
                    (F.coalesce(F.col("guidance_rate"), F.lit(0)).cast(T.DoubleType()) * 0.4 +
                     F.coalesce(F.col("payment_users"), F.lit(0)).cast(T.DoubleType()) / 10 * 0.3 +
                     F.coalesce(F.col("payment_amount"), F.lit(0)).cast(T.DoubleType()) / 1000 * 0.3),
                    F.lit(100)
                )
            ).otherwise(F.lit(0)),
            2
        ).cast(T.DecimalType(5,2)),
        F.lit(0).cast(T.DecimalType(5,2))
    ).alias("guidance_score"),
    F.current_timestamp().cast(T.StringType()).alias("update_time"),
    F.lit(dt).cast(T.StringType()).alias("dt")
)

# 验证数据量
print_data_count(ads_guidance_dashboard, "ads_page_guidance_dashboard")

# 写入数据
ads_guidance_dashboard.write.mode("overwrite") \
    .option("compression", "snappy") \
    .option("parquet.writeLegacyFormat", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd10/ads/ads_page_guidance_dashboard")

# 修复分区
repair_hive_table("ads_page_guidance_dashboard")

# ====================== 综合页面分析看板 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/ads/ads_comprehensive_page_analysis")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS ads_comprehensive_page_analysis")

# 3. 创建外部表 - 修改时间戳类型为字符串以避免兼容性问题
spark.sql("""
CREATE EXTERNAL TABLE ads_comprehensive_page_analysis
(
    `store_id`                 STRING COMMENT '店铺ID',
    `page_type`                STRING COMMENT '页面类型',
    `traffic_score`            DECIMAL(5,2) COMMENT '流量得分(0-100)',
    `click_score`              DECIMAL(5,2) COMMENT '点击得分(0-100)',
    `guidance_score`           DECIMAL(5,2) COMMENT '引导得分(0-100)',
    `overall_score`            DECIMAL(5,2) COMMENT '综合得分(0-100)',
    `grade`                    STRING COMMENT '等级(A/B/C/D)',
    `update_time`              STRING COMMENT '更新时间'
) COMMENT '综合页面分析看板'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd10/ads/ads_comprehensive_page_analysis'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
print("处理综合页面分析看板数据...")

# 读取各维度指标数据
traffic_df = spark.sql(f"SELECT store_id, page_type, traffic_score FROM ads_page_traffic_dashboard WHERE dt = '{dt}'")
click_df = spark.sql(f"SELECT store_id, page_type, click_score FROM ads_page_click_dashboard WHERE dt = '{dt}'")
guidance_df = spark.sql(f"SELECT store_id, page_type, guidance_score FROM ads_page_guidance_dashboard WHERE dt = '{dt}'")

# 合并各维度指标数据
combined_metrics = traffic_df.alias("t1") \
    .join(click_df.alias("t2"),
          (F.col("t1.store_id") == F.col("t2.store_id")) &
          (F.col("t1.page_type") == F.col("t2.page_type")),
          "left") \
    .join(guidance_df.alias("t3"),
          (F.col("t1.store_id") == F.col("t3.store_id")) &
          (F.col("t1.page_type") == F.col("t3.page_type")),
          "left") \
    .select(
    F.coalesce(F.col("t1.store_id"), F.col("t2.store_id"), F.col("t3.store_id"), F.lit("")).cast(T.StringType()).alias("store_id"),
    F.coalesce(F.col("t1.page_type"), F.col("t2.page_type"), F.col("t3.page_type"), F.lit("")).cast(T.StringType()).alias("page_type"),
    F.coalesce(F.col("t1.traffic_score"), F.lit(0)).cast(T.DecimalType(5,2)).alias("traffic_score"),
    F.coalesce(F.col("t2.click_score"), F.lit(0)).cast(T.DecimalType(5,2)).alias("click_score"),
    F.coalesce(F.col("t3.guidance_score"), F.lit(0)).cast(T.DecimalType(5,2)).alias("guidance_score")
)

# 计算综合得分和等级
comprehensive_analysis = combined_metrics.select(
    F.coalesce(F.col("store_id"), F.lit("")).cast(T.StringType()).alias("store_id"),
    F.coalesce(F.col("page_type"), F.lit("")).cast(T.StringType()).alias("page_type"),
    F.coalesce(F.col("traffic_score"), F.lit(0)).cast(T.DecimalType(5,2)).alias("traffic_score"),
    F.coalesce(F.col("click_score"), F.lit(0)).cast(T.DecimalType(5,2)).alias("click_score"),
    F.coalesce(F.col("guidance_score"), F.lit(0)).cast(T.DecimalType(5,2)).alias("guidance_score"),
    # 综合得分计算：流量得分(40%) + 点击得分(30%) + 引导得分(30%)
    F.coalesce(
        F.round(
            F.coalesce(F.col("traffic_score"), F.lit(0)).cast(T.DoubleType()) * 0.4 +
            F.coalesce(F.col("click_score"), F.lit(0)).cast(T.DoubleType()) * 0.3 +
            F.coalesce(F.col("guidance_score"), F.lit(0)).cast(T.DoubleType()) * 0.3,
            2
        ).cast(T.DecimalType(5,2)),
        F.lit(0).cast(T.DecimalType(5,2))
    ).alias("overall_score"),
    F.lit(dt).cast(T.StringType()).alias("dt")
).withColumn(
    "grade",
    F.when(F.col("overall_score") >= 85, "A")
    .when(F.col("overall_score") >= 70, "B")
    .when(F.col("overall_score") >= 50, "C")
    .otherwise("D")
).withColumn(
    "update_time",
    F.current_timestamp().cast(T.StringType())
)

# 验证数据量
print_data_count(comprehensive_analysis, "ads_comprehensive_page_analysis")

# 显示部分数据以检查结果
print("ADS层综合页面分析看板前10条数据:")
comprehensive_analysis.show(10)

# 统计得分分布
print("各项得分统计信息:")
comprehensive_analysis.select(
    F.min("traffic_score").alias("min_traffic_score"),
    F.max("traffic_score").alias("max_traffic_score"),
    F.avg("traffic_score").alias("avg_traffic_score"),
    F.min("click_score").alias("min_click_score"),
    F.max("click_score").alias("max_click_score"),
    F.avg("click_score").alias("avg_click_score"),
    F.min("overall_score").alias("min_overall_score"),
    F.max("overall_score").alias("max_overall_score"),
    F.avg("overall_score").alias("avg_overall_score")
).show()

# 显示等级分布
print("等级分布:")
comprehensive_analysis.groupBy("grade").count().orderBy("grade").show()

# 写入数据
comprehensive_analysis.write.mode("overwrite") \
    .option("compression", "snappy") \
    .option("parquet.writeLegacyFormat", "true") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd10/ads/ads_comprehensive_page_analysis")

# 修复分区
repair_hive_table("ads_comprehensive_page_analysis")

# 验证最终结果
print("验证最终结果:")
traffic_dashboard_count = spark.sql(f"SELECT COUNT(*) as count FROM ads_page_traffic_dashboard WHERE dt = '{dt}'").collect()[0]['count']
click_dashboard_count = spark.sql(f"SELECT COUNT(*) as count FROM ads_page_click_dashboard WHERE dt = '{dt}'").collect()[0]['count']
guidance_dashboard_count = spark.sql(f"SELECT COUNT(*) as count FROM ads_page_guidance_dashboard WHERE dt = '{dt}'").collect()[0]['count']
comprehensive_count = spark.sql(f"SELECT COUNT(*) as count FROM ads_comprehensive_page_analysis WHERE dt = '{dt}'").collect()[0]['count']

print(f"流量分析看板记录数: {traffic_dashboard_count}")
print(f"点击分布看板记录数: {click_dashboard_count}")
print(f"引导效果看板记录数: {guidance_dashboard_count}")
print(f"综合分析看板记录数: {comprehensive_count}")

# 显示部分结果
print("综合页面分析看板前5条数据:")
spark.sql(f"SELECT * FROM ads_comprehensive_page_analysis WHERE dt = '{dt}' LIMIT 5").show()

print("GD10 ADS层数据处理完成!")
spark.stop()
