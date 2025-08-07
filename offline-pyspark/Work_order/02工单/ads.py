from pyspark.sql import SparkSession
import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Ecommerce_ADS_Tables") \
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
spark.sql("USE gd02")

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
    spark.sql(f"MSCK REPAIR TABLE gd02.{table_name}")
    print(f"修复分区完成：gd02.{table_name}")

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# 处理日期参数
process_date = "20250101"  # 与DWS层保持一致的处理日期
process_date_ymd = datetime.datetime.strptime(process_date, "%Y%m%d").strftime("%Y-%m-%d")

# 计算昨日日期（用于环比计算）
yesterday = (datetime.datetime.strptime(process_date, "%Y%m%d") - datetime.timedelta(days=1)).strftime("%Y%m%d")
yesterday_ymd = datetime.datetime.strptime(yesterday, "%Y%m%d").strftime("%Y-%m-%d")


# ====================== 平台核心指标表 ads_platform_core_index ======================
create_hdfs_dir("/warehouse/gd02/ads/ads_platform_core_index")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.ads_platform_core_index (
    stat_date STRING COMMENT '统计日期',
    total_visitor_num INT COMMENT '总访客数',
    total_visit_num INT COMMENT '总访问次数',
    total_order_num INT COMMENT '总订单数',
    total_sales_amount DOUBLE COMMENT '总销售额',
    total_sales_num INT COMMENT '总销量',
    avg_order_amount DOUBLE COMMENT '平均订单金额',
    pay_conversion_rate DOUBLE COMMENT '支付转化率(%)',
    visitor_avg_visit_num DOUBLE COMMENT '访客平均访问次数',
    yoy_growth_rate DOUBLE COMMENT '同比增长率(%)',
    mom_growth_rate DOUBLE COMMENT '环比增长率(%)'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/ads/ads_platform_core_index'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 计算当日核心指标
current_day_data = spark.table("gd02.dws_product_visit_summary").alias("visit") \
    .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
    .select(
    F.sum("total_visitor_num").alias("total_visitor_num"),
    F.sum("total_visit_num").alias("total_visit_num")
) \
    .crossJoin(
    spark.table("gd02.dws_product_sale_summary").alias("sale")
    .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day"))
    .select(
        F.sum("total_sales_amount").alias("total_sales_amount"),
        F.sum("total_sales_num").alias("total_sales_num"),
        F.sum("total_order_num").alias("total_order_num")
    )
) \
    .withColumn("avg_order_amount", F.round(F.col("total_sales_amount") / F.col("total_order_num"), 2)) \
    .withColumn("pay_conversion_rate", F.round((F.col("total_order_num") / F.col("total_visitor_num")) * 100, 2)) \
    .withColumn("visitor_avg_visit_num", F.round(F.col("total_visit_num") / F.col("total_visitor_num"), 2)) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day"))

# 计算环比（与昨日对比）
yesterday_data = spark.table("gd02.dws_product_sale_summary").alias("sale") \
    .filter((F.col("stat_date") == yesterday_ymd) & (F.col("stat_period") == "day")) \
    .select(F.sum("total_sales_amount").alias("yesterday_sales"))

# 合并环比计算
final_core_index = current_day_data.crossJoin(yesterday_data) \
    .withColumn(
    "mom_growth_rate",
    F.round(
        ((F.col("total_sales_amount") - F.col("yesterday_sales")) / F.col("yesterday_sales")) * 100,
        2
    )
) \
    .withColumn("yoy_growth_rate", F.lit(0.0)) \
    .select(
    "stat_date", "total_visitor_num", "total_visit_num",
    "total_order_num", "total_sales_amount", "total_sales_num",
    "avg_order_amount", "pay_conversion_rate", "visitor_avg_visit_num",
    "yoy_growth_rate", "mom_growth_rate", "stat_period"
)

print_data_count(final_core_index, "ads_platform_core_index")

# 写入数据
final_core_index.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/ads/ads_platform_core_index")

repair_hive_table("ads_platform_core_index")


# ====================== 商品分类销售排行 ads_category_sale_ranking ======================
create_hdfs_dir("/warehouse/gd02/ads/ads_category_sale_ranking")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.ads_category_sale_ranking (
    stat_date STRING COMMENT '统计日期',
    category_id INT COMMENT '分类ID',
    category_name STRING COMMENT '分类名称',
    sales_amount DOUBLE COMMENT '分类销售额',
    sales_num INT COMMENT '分类销量',
    sales_ratio DOUBLE COMMENT '销售额占比(%)',
    rank INT COMMENT '销售排名'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/ads/ads_category_sale_ranking'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 计算分类销售数据
category_sales = spark.table("gd02.dws_product_sale_summary").alias("sale") \
    .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
    .groupBy("category_id", "category_name") \
    .agg(
    F.sum("total_sales_amount").alias("sales_amount"),
    F.sum("total_sales_num").alias("sales_num")
)

# 计算总销售额（用于计算占比）
total_sales = category_sales.agg(F.sum("sales_amount").alias("total_sales")).collect()[0]["total_sales"]

# 计算排名和占比
category_ranking = category_sales \
    .withColumn("sales_ratio", F.round((F.col("sales_amount") / total_sales) * 100, 2)) \
    .withColumn(
    "rank",
    F.row_number().over(Window.orderBy(F.col("sales_amount").desc()))
) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day"))

print_data_count(category_ranking, "ads_category_sale_ranking")

# 写入数据
category_ranking.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/ads/ads_category_sale_ranking")

repair_hive_table("ads_category_sale_ranking")


# ====================== 热门商品排行 ads_hot_product_ranking ======================
create_hdfs_dir("/warehouse/gd02/ads/ads_hot_product_ranking")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.ads_hot_product_ranking (
    stat_date STRING COMMENT '统计日期',
    product_id INT COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '分类名称',
    sales_amount DOUBLE COMMENT '商品销售额',
    visit_num INT COMMENT '商品访问量',
    conversion_rate DOUBLE COMMENT '转化率(%)',
    rank INT COMMENT '综合排名'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/ads/ads_hot_product_ranking'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 关联销售和访问数据
product_data = spark.table("gd02.dws_product_sale_summary").alias("sale") \
    .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
    .join(
    spark.table("gd02.dws_product_visit_summary").alias("visit")
    .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")),
    on="product_id",
    how="inner"
) \
    .select(
    F.col("sale.product_id"),
    F.col("sale.product_name"),
    F.col("sale.category_name"),
    F.col("sale.total_sales_amount").alias("sales_amount"),
    F.col("visit.total_visit_num").alias("visit_num"),
    F.col("visit.pay_conversion_rate").alias("conversion_rate")
)

# 计算综合排名（基于销售额和转化率）
hot_product_ranking = product_data \
    .withColumn(
    "rank",
    F.row_number().over(
        Window.orderBy(
            F.col("sales_amount").desc(),
            F.col("conversion_rate").desc()
        )
    )
) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day"))

print_data_count(hot_product_ranking, "ads_hot_product_ranking")

# 写入数据
hot_product_ranking.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/ads/ads_hot_product_ranking")

repair_hive_table("ads_hot_product_ranking")


# ====================== 热门搜索词排行 ads_hot_search_ranking ======================
create_hdfs_dir("/warehouse/gd02/ads/ads_hot_search_ranking")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.ads_hot_search_ranking (
    stat_date STRING COMMENT '统计日期',
    search_word STRING COMMENT '搜索词',
    search_num INT COMMENT '搜索次数',
    click_rate DOUBLE COMMENT '点击率(%)',
    conversion_rate DOUBLE COMMENT '转化率(%)',
    rank INT COMMENT '搜索热度排名'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/ads/ads_hot_search_ranking'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 基础搜索数据
search_base = spark.table("gd02.dws_search_word_summary").alias("search") \
    .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day"))

# 计算点击量（从访问日志）
search_click = spark.table("gd02.dwd_product_visit_detail").alias("visit") \
    .filter(F.col("dt") == process_date) \
    .join(
    spark.table("gd02.dwd_product_search_detail").alias("search")
    .filter(F.col("dt") == process_date),
    on=["user_id", "product_id"],
    how="inner"
) \
    .groupBy("search_word") \
    .agg(F.count("*").alias("click_num"))

# 计算转化率（从销售日志）
search_conversion = spark.table("gd02.dwd_product_sale_detail").alias("sale") \
    .filter(F.col("dt") == process_date) \
    .join(
    spark.table("gd02.dwd_product_search_detail").alias("search")
    .filter(F.col("dt") == process_date),
    on=["user_id", "product_id"],
    how="inner"
) \
    .groupBy("search_word") \
    .agg(F.countDistinct("order_id").alias("conversion_num"))

# 合并计算排名
hot_search_ranking = search_base \
    .join(search_click, on="search_word", how="left") \
    .join(search_conversion, on="search_word", how="left") \
    .withColumn("click_rate", F.round((F.col("click_num") / F.col("total_search_num")) * 100, 2)) \
    .withColumn("conversion_rate", F.round((F.col("conversion_num") / F.col("total_search_num")) * 100, 2)) \
    .withColumn(
    "rank",
    F.row_number().over(Window.orderBy(F.col("total_search_num").desc()))
) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day")) \
    .fillna(0, subset=["click_rate", "conversion_rate"]) \
    .select(
    "stat_date", "search_word", "total_search_num",
    "click_rate", "conversion_rate", "rank", "stat_period"
) \
    .withColumnRenamed("total_search_num", "search_num")

print_data_count(hot_search_ranking, "ads_hot_search_ranking")

# 写入数据
hot_search_ranking.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/ads/ads_hot_search_ranking")

repair_hive_table("ads_hot_search_ranking")


print("所有ADS层表创建及数据导入完成！")
spark.stop()