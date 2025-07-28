from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ADS_Trans_Order_Stats") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_ads.ads_trans_order_stats")

# 2. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_ads.ads_trans_order_stats
(
    `dt`                    string COMMENT '统计日期',
    `recent_days`           tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `receive_order_count`   bigint COMMENT '接单总数',
    `receive_order_amount`  decimal(16, 2) COMMENT '接单金额',
    `dispatch_order_count`  bigint COMMENT '发单总数',
    `dispatch_order_amount` decimal(16, 2) COMMENT '发单金额'
) comment '运单相关统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
    LOCATION '/warehouse/tms_spark/ads/ads_trans_order_stats'
""")

# 3. 处理最近1天的数据
receive_1d_df = spark.table("tms_spark_dws.dws_trans_org_receive_1d") \
    .filter(F.col("dt") == "2025-07-11") \
    .select(
    F.lit(1).alias("recent_days"),
    F.sum("order_count").alias("receive_order_count"),
    F.sum("order_amount").alias("receive_order_amount")
)

dispatch_1d_df = spark.table("tms_spark_dws.dws_trans_dispatch_1d") \
    .filter(F.col("dt") == "2025-07-11") \
    .select(
    F.lit(1).alias("recent_days"),
    F.col("order_count").alias("dispatch_order_count"),
    F.col("order_amount").alias("dispatch_order_amount")
)

# 4. 连接最近1天的数据
joined_1d_df = receive_1d_df.alias("receive_1d") \
    .join(
    dispatch_1d_df.alias("dispatch_1d"),
    on=F.col("receive_1d.recent_days") == F.col("dispatch_1d.recent_days"),
    how="full_outer"
) \
    .select(

    F.lit("20250713").alias("dt"),
    F.coalesce(F.col("receive_1d.recent_days"), F.col("dispatch_1d.recent_days")).alias("recent_days"),
    F.col("receive_order_count"),
    F.col("receive_order_amount"),
    F.col("dispatch_order_count"),
    F.col("dispatch_order_amount")
)

# 5. 处理最近n天的数据
receive_nd_df = spark.table("tms_spark_dws.dws_trans_org_receive_nd") \
    .filter(F.col("dt") == "2023-01-10") \
    .groupBy("recent_days") \
    .agg(
    F.sum("order_count").alias("receive_order_count"),
    F.sum("order_amount").alias("receive_order_amount")
)

dispatch_nd_df = spark.table("tms_spark_dws.dws_trans_dispatch_nd") \
    .filter(F.col("dt") == "20250713") \
    .select(
    F.col("recent_days"),
    F.col("order_count").alias("dispatch_order_count"),
    F.col("order_amount").alias("dispatch_order_amount")
)

# 6. 连接最近n天的数据
joined_nd_df = receive_nd_df.alias("receive_nd") \
    .join(
    dispatch_nd_df.alias("dispatch_nd"),
    on=F.col("receive_nd.recent_days") == F.col("dispatch_nd.recent_days"),
    how="full_outer"
) \
    .select(
    F.lit("20250713").alias("dt"),
    F.coalesce(F.col("receive_nd.recent_days"), F.col("dispatch_nd.recent_days")).alias("recent_days"),
    F.col("receive_order_count"),
    F.col("receive_order_amount"),
    F.col("dispatch_order_count"),
    F.col("dispatch_order_amount")
)

# 7. 合并数据并写入
final_df = joined_1d_df.union(joined_nd_df)
final_df.write.mode("overwrite").saveAsTable("tms_spark_ads.ads_trans_order_stats")