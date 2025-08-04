# -*- coding: utf-8 -*-
# 工单编号：大数据-电商数仓-10-流量主题页面分析看板

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("DWS_Layer_Aggregation") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "D:/spark_temp") \
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
    spark.sql(f"MSCK REPAIR TABLE gd10.{table_name}")
    print(f"修复分区完成：gd10.{table_name}")

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# 添加数据质量检查
print("=== 数据质量检查 ===")
# 检查DWD层数据总量
total_count = spark.sql("SELECT COUNT(*) as count FROM gd10.dwd_user_behavior_log WHERE dt = '20250804'").collect()[0]['count']
print(f"DWD层总记录数: {total_count}")

# 检查各事件类型分布
print("各事件类型分布:")
spark.sql("""
    SELECT event_type, COUNT(*) as count 
    FROM gd10.dwd_user_behavior_log 
    WHERE dt = '20250804' 
    GROUP BY event_type 
    ORDER BY count DESC
""").show()

# 检查page_type分布
print("各页面类型分布:")
spark.sql("""
    SELECT page_type, COUNT(*) as count 
    FROM gd10.dwd_user_behavior_log 
    WHERE dt = '20250804' 
    GROUP BY page_type 
    ORDER BY count DESC
""").show()

# 检查搜索排名统计
print("搜索排名统计:")
spark.sql("""
    SELECT 
        COUNT(*) as total_search_events,
        AVG(search_rank) as avg_search_rank,
        MIN(search_rank) as min_search_rank,
        MAX(search_rank) as max_search_rank
    FROM gd10.dwd_user_behavior_log 
    WHERE dt = '20250804' AND event_type = 'search' AND search_rank IS NOT NULL
""").show()

# 检查有数据的店铺和商品
print("有数据的店铺商品组合数:")
spark.sql("""
    SELECT COUNT(*) as count 
    FROM (
        SELECT store_id, item_id 
        FROM gd10.dwd_user_behavior_log 
        WHERE dt = '20250804' AND store_id IS NOT NULL AND item_id IS NOT NULL
        GROUP BY store_id, item_id
    ) t
""").show()

# ====================== DWS层页面流量分析日汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/dws/dws_page_traffic_analysis_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS gd10.dws_page_traffic_analysis_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE gd10.dws_page_traffic_analysis_d (
    `store_id`             STRING COMMENT '店铺ID',
    `page_type`            STRING COMMENT '页面类型(home-首页,item_list-商品列表,item_detail-商品详情,search_result-搜索结果)',
    `page_views`           BIGINT COMMENT '页面浏览量',
    `unique_visitors`      BIGINT COMMENT '独立访客数',
    `avg_stay_duration`    DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
    `bounce_rate`          DECIMAL(5, 2) COMMENT '跳出率(%)',
    `search_count`         BIGINT COMMENT '搜索次数',
    `avg_search_rank`      DECIMAL(5, 2) COMMENT '平均搜索排名',
    `update_time`          TIMESTAMP COMMENT '更新时间'
) comment '页面流量分析日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')STORED AS ORC
LOCATION '/warehouse/gd10/dws/dws_page_traffic_analysis_d'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
page_traffic_df = spark.table("gd10.dwd_user_behavior_log").filter(
    (F.col("dt") == "20250804") &
    (F.col("store_id").isNotNull()) &
    (F.col("page_type").isNotNull())
).groupBy("store_id", "page_type").agg(
    F.count(F.when(F.col("event_type") == "view", F.lit(1))).alias("page_views"),
    F.countDistinct(F.when(F.col("event_type") == "view", F.col("user_id"))).alias("unique_visitors"),
    F.round(
        F.coalesce(
            F.avg(F.when(F.col("event_type") == "view", F.col("duration"))),
            F.lit(0)
        ),
        2
    ).alias("avg_stay_duration"),
    F.round(
        F.coalesce(
            F.count(F.when((F.col("event_type") == "view") & (F.col("duration") < 10), F.lit(1))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.count(F.when(F.col("event_type") == "view", F.lit(1))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("bounce_rate"),
    F.count(F.when(F.col("event_type") == "search", F.lit(1))).alias("search_count"),
    F.round(
        F.coalesce(
            F.avg(F.when(F.col("event_type") == "search", F.col("search_rank"))),
            F.lit(0)
        ),
        2
    ).alias("avg_search_rank"),
    F.current_timestamp().alias("update_time")
).withColumn("dt", F.lit("20250804"))

# 5. 验证数据量
print_data_count(page_traffic_df, "dws_page_traffic_analysis_d")

# 6. 写入数据
page_traffic_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd10/dws/dws_page_traffic_analysis_d")

# 7. 修复分区
repair_hive_table("dws_page_traffic_analysis_d")

# ====================== DWS层页面点击分布日汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/dws/dws_page_click_distribution_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS gd10.dws_page_click_distribution_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE gd10.dws_page_click_distribution_d (
    `store_id`             STRING COMMENT '店铺ID',
    `page_type`            STRING COMMENT '页面类型(home-首页,item_list-商品列表,item_detail-商品详情,search_result-搜索结果)',
    `click_count`          BIGINT COMMENT '点击次数',
    `click_users`          BIGINT COMMENT '点击用户数',
    `conversion_clicks`    BIGINT COMMENT '转化点击数(加购/支付)',
    `conversion_rate`      DECIMAL(5, 2) COMMENT '转化率(%)',
    `update_time`          TIMESTAMP COMMENT '更新时间'
) comment '页面点击分布日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')STORED AS ORC
LOCATION '/warehouse/gd10/dws/dws_page_click_distribution_d'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理 - 修复后的代码
click_distribution_df = spark.table("gd10.dwd_user_behavior_log").filter(
    (F.col("dt") == "20250804") &
    (F.col("store_id").isNotNull()) &
    (F.col("page_type").isNotNull())
).groupBy("store_id", "page_type").agg(
    F.count(F.when(F.col("event_type") == "view", F.lit(1))).alias("click_count"),
    F.countDistinct(F.when(F.col("event_type") == "view", F.col("user_id"))).alias("click_users"),
    F.count(F.when((F.col("event_type") == "add_to_cart") | (F.col("event_type") == "payment"), F.lit(1))).alias("conversion_clicks"),
    F.round(
        F.coalesce(
            F.count(F.when((F.col("event_type") == "add_to_cart") | (F.col("event_type") == "payment"), F.lit(1))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.count(F.when(F.col("event_type") == "view", F.lit(1))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("conversion_rate"),
    F.current_timestamp().alias("update_time")
).withColumn("dt", F.lit("20250804"))

# 5. 验证数据量
print_data_count(click_distribution_df, "dws_page_click_distribution_d")

# 6. 写入数据
click_distribution_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd10/dws/dws_page_click_distribution_d")

# 7. 修复分区
repair_hive_table("dws_page_click_distribution_d")

# ====================== DWS层页面引导效果日汇总表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/dws/dws_page_guidance_effect_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS gd10.dws_page_guidance_effect_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE gd10.dws_page_guidance_effect_d (
    `store_id`                 STRING COMMENT '店铺ID',
    `page_type`                STRING COMMENT '页面类型(home-首页,item_list-商品列表,item_detail-商品详情,search_result-搜索结果)',
    `page_views`               BIGINT COMMENT '页面浏览量',
    `to_item_detail_views`     BIGINT COMMENT '引导至商品详情页浏览量',
    `guidance_rate`            DECIMAL(5, 2) COMMENT '引导率(%)',
    `payment_users`            BIGINT COMMENT '引导支付用户数',
    `payment_amount`           DECIMAL(15, 2) COMMENT '引导支付金额',
    `update_time`              TIMESTAMP COMMENT '更新时间'
) comment '页面引导效果日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')STORED AS ORC
LOCATION '/warehouse/gd10/dws/dws_page_guidance_effect_d'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理 - 修复后的代码
guidance_effect_df = spark.table("gd10.dwd_user_behavior_log").filter(
    (F.col("dt") == "20250804") &
    (F.col("store_id").isNotNull()) &
    (F.col("page_type").isNotNull())
).groupBy("store_id", "page_type").agg(
    F.count(F.when(F.col("event_type") == "view", F.lit(1))).alias("page_views"),
    F.count(F.when((F.col("page_type") != "item_detail") & (F.col("event_type") == "view") & (F.col("page_type") == "item_list"), F.lit(1))).alias("to_item_detail_views"),
    F.round(
        F.coalesce(
            F.count(F.when((F.col("page_type") != "item_detail") & (F.col("event_type") == "view") & (F.col("page_type") == "item_list"), F.lit(1))).cast(T.DoubleType()) * 100.0 /
            F.coalesce(
                F.count(F.when(F.col("event_type") == "view", F.lit(1))).cast(T.DoubleType()),
                F.lit(1)
            ),
            F.lit(0)
        ),
        2
    ).alias("guidance_rate"),
    F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))).alias("payment_users"),
    F.round(
        F.coalesce(
            F.sum(F.when(F.col("event_type") == "payment", F.col("acquisition_cost"))),
            F.lit(0)
        ),
        2
    ).alias("payment_amount"),
    F.current_timestamp().alias("update_time")
).withColumn("dt", F.lit("20250804"))

# 5. 验证数据量
print_data_count(guidance_effect_df, "dws_page_guidance_effect_d")

# 6. 写入数据
guidance_effect_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/gd10/dws/dws_page_guidance_effect_d")

# 7. 修复分区
repair_hive_table("dws_page_guidance_effect_d")

# 验证DWS层数据
spark.sql("""
SELECT
    'DWS Layer Record Counts' as info,
    (SELECT COUNT(*) FROM gd10.dws_page_traffic_analysis_d WHERE dt = '20250804') as traffic_count,
    (SELECT COUNT(*) FROM gd10.dws_page_click_distribution_d WHERE dt = '20250804') as click_count,
    (SELECT COUNT(*) FROM gd10.dws_page_guidance_effect_d WHERE dt = '20250804') as guidance_count
""").show()

# 查看部分结果示例
spark.sql("SELECT * FROM gd10.dws_page_traffic_analysis_d WHERE dt = '20250804' LIMIT 5").show()
spark.sql("SELECT * FROM gd10.dws_page_click_distribution_d WHERE dt = '20250804' LIMIT 5").show()
spark.sql("SELECT * FROM gd10.dws_page_guidance_effect_d WHERE dt = '20250804' LIMIT 5").show()

spark.stop()
