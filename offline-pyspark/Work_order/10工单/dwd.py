# -*- coding: utf-8 -*-
# 工单编号：大数据-电商数仓-10-流量主题页面分析看板

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD10 DWD Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "D:/spark_temp") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置当前数据库
spark.sql("USE gd10")

print("开始处理GD10 DWD层数据...")

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

# ====================== DWD层用户行为明细表 ======================
print("处理用户行为明细表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/dwd/dwd_user_behavior_log")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dwd_user_behavior_log")

# 3. 创建外部表 - 使用更兼容的数据类型
spark.sql("""
CREATE EXTERNAL TABLE dwd_user_behavior_log
(
    `user_id`          STRING COMMENT '用户ID',
    `session_id`       STRING COMMENT '会话ID',
    `store_id`         STRING COMMENT '店铺ID',
    `item_id`          STRING COMMENT '商品ID',
    `page_type`        STRING COMMENT '页面类型(home-首页,item_list-商品列表,item_detail-商品详情,search_result-搜索结果)',
    `event_type`       STRING COMMENT '事件类型(view-浏览,search-搜索,add_to_cart-加购,payment-支付,payment_success-支付成功,content_view-内容浏览,content_share-内容分享,comment-评论)',
    `event_time`       STRING COMMENT '事件时间',
    `search_rank`      INT COMMENT '搜索排名',
    `duration`         INT COMMENT '停留时长(秒)',
    `is_new_customer`  INT COMMENT '是否新客户(1:是,0:否)',
    `repurchase_flag`  INT COMMENT '复购标识(1:是,0:否)',
    `acquisition_cost` DECIMAL(10, 2) COMMENT '获取成本',
    `rating`           INT COMMENT '评分(1-5分)',
    `update_time`      STRING COMMENT '更新时间'
) COMMENT '用户行为明细表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd10/dwd/dwd_user_behavior_log'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 读取ODS层数据 - 保持最宽松的过滤条件
print("读取ODS层用户行为数据...")
ods_user_behavior = spark.sql("""
    SELECT * FROM ods_user_behavior_log 
    WHERE dt = '20250804'
""")

print(f"ODS层原始数据量: {ods_user_behavior.count()}")

# 5. 查看原始数据分布 - 增加诊断信息
print("查看ODS层原始字段值分布:")
print("原始page_type分布:")
ods_user_behavior.groupBy("page_type").count().orderBy(F.desc("count")).show(20)

print("原始event_type分布:")
ods_user_behavior.groupBy("event_type").count().orderBy(F.desc("count")).show(20)

print("原始search_rank分布(前20条):")
ods_user_behavior.select("search_rank").filter(F.col("search_rank").isNotNull()).distinct().show(20)

# 6. 数据清洗和标准化处理 - 添加映射逻辑
print("清洗和标准化用户行为数据...")
dwd_user_behavior_log = ods_user_behavior.select(
    # 字符串字段 - 最宽松处理，几乎不做验证
    F.when(F.col("user_id").isNotNull(), F.col("user_id").cast("string"))
    .otherwise(F.lit("UNKNOWN_USER"))
    .alias("user_id"),

    F.when(F.col("session_id").isNotNull(), F.col("session_id").cast("string"))
    .otherwise(F.lit("UNKNOWN_SESSION"))
    .alias("session_id"),

    F.when(F.col("store_id").isNotNull(), F.col("store_id").cast("string"))
    .otherwise(F.lit("UNKNOWN_STORE"))
    .alias("store_id"),

    F.when(F.col("item_id").isNotNull(), F.col("item_id").cast("string"))
    .otherwise(F.lit("UNKNOWN_ITEM"))
    .alias("item_id"),

    # page_type字段 - 数字映射到字符串
    F.when(F.col("page_type") == 1, "home")
    .when(F.col("page_type") == 2, "item_list")
    .when(F.col("page_type") == 3, "item_detail")
    .when(F.col("page_type") == 4, "search_result")
    .when(F.col("page_type") == 5, "content")
    .otherwise("home")
    .alias("page_type"),

    # event_type字段 - 数字映射到字符串
    F.when(F.col("event_type") == 1, "view")
    .when(F.col("event_type") == 2, "search")
    .when(F.col("event_type") == 3, "add_to_cart")
    .when(F.col("event_type") == 4, "payment")
    .when(F.col("event_type") == 5, "payment_success")
    .otherwise("view")
    .alias("event_type"),

    # 时间字段 - 简单转换
    F.when(F.col("event_time").isNotNull(), F.col("event_time").cast("string"))
    .otherwise(F.lit("1900-01-01 00:00:00"))
    .alias("event_time"),

    # 数值字段 - 最宽松处理
    F.when(F.col("search_rank").isNotNull(), F.col("search_rank").cast("int"))
    .otherwise(None)
    .alias("search_rank"),

    F.when(F.col("duration").isNotNull(), F.col("duration").cast("int"))
    .otherwise(0)
    .alias("duration"),

    # 布尔字段 - 最宽松处理
    F.when(F.col("is_new_customer").isNotNull(), F.col("is_new_customer").cast("int"))
    .otherwise(0)
    .alias("is_new_customer"),

    F.when(F.col("repurchase_flag").isNotNull(), F.col("repurchase_flag").cast("int"))
    .otherwise(0)
    .alias("repurchase_flag"),

    # 金额字段 - 最宽松处理
    F.when(F.col("acquisition_cost").isNotNull(), F.col("acquisition_cost").cast("decimal(10,2)"))
    .otherwise(F.lit(0.00).cast("decimal(10,2)"))
    .alias("acquisition_cost"),

    # 评分字段 - 最宽松处理
    F.when(F.col("rating").isNotNull(), F.col("rating").cast("int"))
    .otherwise(3)
    .alias("rating"),

    # 更新时间字段
    F.when(F.col("update_time").isNotNull(), F.col("update_time").cast("string"))
    .otherwise(F.lit("1900-01-01 00:00:00"))
    .alias("update_time"),

    # 分区字段
    F.lit("20250804").alias("dt")
)

# 7. 数据质量检查 - 增加更多检查
print("执行数据质量检查...")
print("DWD层数据样本:")
dwd_user_behavior_log.show(5)

print("处理后event_type分布:")
dwd_user_behavior_log.groupBy("event_type").count().orderBy(F.desc("count")).show(20)

print("处理后page_type分布:")
dwd_user_behavior_log.groupBy("page_type").count().orderBy(F.desc("count")).show(20)

print("search_rank非空值统计:")
dwd_user_behavior_log.filter(F.col("search_rank").isNotNull()).select(
    F.count("*").alias("count"),
    F.avg("search_rank").alias("avg"),
    F.min("search_rank").alias("min"),
    F.max("search_rank").alias("max")
).show()

# 8. 写入数据
print("写入用户行为明细表数据...")
dwd_user_behavior_log.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd10/dwd/dwd_user_behavior_log")

# 9. 修复分区
repair_hive_table("dwd_user_behavior_log")

print(f"用户行为明细表处理完成，记录数: {dwd_user_behavior_log.count()}")

# ====================== 数据验证 ======================
print("验证DWD层数据:")
try:
    behavior_count = spark.sql("SELECT COUNT(*) as count FROM dwd_user_behavior_log WHERE dt = '20250804'").collect()[0]['count']
    print(f"用户行为明细表记录数: {behavior_count}")

    # 检查page_type分布
    print("page_type分布:")
    spark.sql("""
        SELECT page_type, COUNT(*) as count 
        FROM dwd_user_behavior_log 
        WHERE dt = '20250804' 
        GROUP BY page_type 
        ORDER BY count DESC
    """).show()

    # 检查event_type分布
    print("event_type分布:")
    spark.sql("""
        SELECT event_type, COUNT(*) as count 
        FROM dwd_user_behavior_log 
        WHERE dt = '20250804' 
        GROUP BY event_type 
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
        FROM dwd_user_behavior_log 
        WHERE dt = '20250804' AND event_type = 'search' AND search_rank IS NOT NULL
    """).show()

except Exception as e:
    print(f"验证DWD层数据时出错: {e}")

print("GD10 DWD层数据处理完成!")
spark.stop()
