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
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.files.ignoreCorruptFiles", "true") \
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

def force_delete_hdfs_path(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if fs.exists(jvm_path):
        fs.delete(jvm_path, True)
        print(f"已强制删除HDFS路径及所有内容：{path}")
    else:
        print(f"HDFS路径不存在：{path}")


# 处理日期参数
process_date = "20250105"  # 可根据需要修改为具体日期
process_date_ymd = datetime.datetime.strptime(process_date, "%Y%m%d").strftime("%Y-%m-%d")

# 计算昨日日期
yesterday = (datetime.datetime.strptime(process_date, "%Y%m%d") - datetime.timedelta(days=1)).strftime("%Y%m%d")
yesterday_ymd = datetime.datetime.strptime(yesterday, "%Y%m%d").strftime("%Y-%m-%d")

# 计算7天前日期
seven_days_ago = (datetime.datetime.strptime(process_date, "%Y%m%d") - datetime.timedelta(days=7)).strftime("%Y%m%d")


# ====================== 平台核心指标表 ads_platform_core_index ======================
create_hdfs_dir("/warehouse/gd02/ads/ads_platform_core_index")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.ads_platform_core_index (
    stat_date STRING COMMENT '统计日期',
    total_visitor_num BIGINT COMMENT '总访客数',
    total_visit_num BIGINT COMMENT '总访问次数',
    total_order_num BIGINT COMMENT '总订单数',
    total_sales_amount DOUBLE COMMENT '总销售额',
    avg_order_amount DOUBLE COMMENT '平均订单金额',
    pay_conversion_rate DOUBLE COMMENT '支付转化率(%)',
    visitor_avg_stay_time DOUBLE COMMENT '访客平均停留时间(秒)',
    yoy_growth_rate DOUBLE COMMENT '同比增长率(%)',
    mom_growth_rate DOUBLE COMMENT '环比增长率(%)'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/ads/ads_platform_core_index'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 计算当日核心指标，处理数据类型转换
try:
    daily_visit = spark.read.option("mergeSchema", "false").table("gd02.dws_product_visit_summary") \
        .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
        .select(
        F.col("total_visitor_num").cast("bigint").alias("total_visitor_num"),
        F.col("total_visit_num").cast("bigint").alias("total_visit_num")
    ) \
        .agg(
        F.sum("total_visitor_num").alias("total_visitor_num"),
        F.sum("total_visit_num").alias("total_visit_num")
    )
except Exception as e:
    print(f"读取dws_product_visit_summary表出错: {e}")
    # 如果读取失败，尝试使用sql方式
    daily_visit = spark.sql(f"""
        SELECT 
            SUM(total_visitor_num) as total_visitor_num,
            SUM(total_visit_num) as total_visit_num
        FROM gd02.dws_product_visit_summary 
        WHERE stat_date = '{process_date_ymd}' AND stat_period = 'day'
    """)

try:
    daily_sale = spark.read.option("mergeSchema", "false").table("gd02.dws_product_sale_summary") \
        .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
        .select(
        F.col("total_order_num").cast("bigint").alias("total_order_num"),
        F.col("total_sales_amount").cast("double").alias("total_sales_amount")
    ) \
        .agg(
        F.sum("total_order_num").alias("total_order_num"),
        F.sum("total_sales_amount").alias("total_sales_amount")
    )
except Exception as e:
    print(f"读取dws_product_sale_summary表出错: {e}")
    # 如果读取失败，尝试使用sql方式
    daily_sale = spark.sql(f"""
        SELECT 
            SUM(total_order_num) as total_order_num,
            SUM(total_sales_amount) as total_sales_amount
        FROM gd02.dws_product_sale_summary 
        WHERE stat_date = '{process_date_ymd}' AND stat_period = 'day'
    """)

# 计算平均停留时间
avg_stay_time = spark.table("gd02.dwd_product_visit_detail") \
    .filter(F.col("dt") == process_date) \
    .agg(F.avg("stay_time").alias("visitor_avg_stay_time"))

# 合并当日指标
daily_core_index = daily_visit.crossJoin(daily_sale).crossJoin(avg_stay_time) \
    .withColumn("avg_order_amount", F.round(F.col("total_sales_amount") / F.col("total_order_num"), 2)) \
    .withColumn("pay_conversion_rate", F.round((F.col("total_order_num") / F.col("total_visitor_num")) * 100, 2)) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day"))

# 计算环比（较昨日）
try:
    yesterday_sale = spark.read.option("mergeSchema", "false").table("gd02.dws_product_sale_summary") \
        .filter((F.col("stat_date") == yesterday_ymd) & (F.col("stat_period") == "day")) \
        .select(F.col("total_sales_amount").cast("double").alias("total_sales_amount")) \
        .agg(F.sum("total_sales_amount").alias("yesterday_sales"))
except Exception as e:
    print(f"读取昨日数据出错: {e}")
    yesterday_sale = spark.sql(f"""
        SELECT 
            SUM(total_sales_amount) as yesterday_sales
        FROM gd02.dws_product_sale_summary 
        WHERE stat_date = '{yesterday_ymd}' AND stat_period = 'day'
    """)

# 计算同比（较上周同期）
last_week_date = (datetime.datetime.strptime(process_date, "%Y%m%d") - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
try:
    last_week_sale = spark.read.option("mergeSchema", "false").table("gd02.dws_product_sale_summary") \
        .filter((F.col("stat_date") == last_week_date) & (F.col("stat_period") == "day")) \
        .select(F.col("total_sales_amount").cast("double").alias("total_sales_amount")) \
        .agg(F.sum("total_sales_amount").alias("last_week_sales"))
except Exception as e:
    print(f"读取上周数据出错: {e}")
    last_week_sale = spark.sql(f"""
        SELECT 
            SUM(total_sales_amount) as last_week_sales
        FROM gd02.dws_product_sale_summary 
        WHERE stat_date = '{last_week_date}' AND stat_period = 'day'
    """)

# 合并增长率
final_core_index = daily_core_index.crossJoin(yesterday_sale).crossJoin(last_week_sale) \
    .withColumn("mom_growth_rate", F.round(
    ((F.col("total_sales_amount") - F.col("yesterday_sales")) / F.col("yesterday_sales")) * 100, 2
).cast("DOUBLE")) \
    .withColumn("yoy_growth_rate", F.round(
    ((F.col("total_sales_amount") - F.col("last_week_sales")) / F.col("last_week_sales")) * 100, 2
).cast("DOUBLE")) \
    .fillna(0, subset=["mom_growth_rate", "yoy_growth_rate"]) \
    .select(
    "stat_date", "total_visitor_num", "total_visit_num", "total_order_num",
    "total_sales_amount", "avg_order_amount", "pay_conversion_rate",
    "visitor_avg_stay_time", "yoy_growth_rate", "mom_growth_rate", "stat_period"
)

print_data_count(final_core_index, "ads_platform_core_index")

# 写入数据
final_core_index.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/ads/ads_platform_core_index")

repair_hive_table("ads_platform_core_index")


# ====================== 商品分类销售排行表 ads_category_sale_ranking ======================
create_hdfs_dir("/warehouse/gd02/ads/ads_category_sale_ranking")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.ads_category_sale_ranking (
    category_id INT COMMENT '分类ID',
    category_name STRING COMMENT '分类名称',
    total_sales_amount DOUBLE COMMENT '总销售额',
    total_sales_num BIGINT COMMENT '总销量',
    sales_rank INT COMMENT '销售排名',
    sales_contribution DOUBLE COMMENT '销售额贡献占比(%)',
    stat_date STRING COMMENT '统计日期'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/ads/ads_category_sale_ranking'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 计算分类销售数据，处理数据类型转换
try:
    category_sale = spark.read.option("mergeSchema", "false").table("gd02.dws_product_sale_summary") \
        .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
        .select(
        F.col("category_id").cast("int").alias("category_id"),
        F.col("category_name"),
        F.col("total_sales_amount").cast("double").alias("total_sales_amount"),
        F.col("total_sales_num").cast("bigint").alias("total_sales_num")
    ) \
        .groupBy("category_id", "category_name") \
        .agg(
        F.sum("total_sales_amount").alias("total_sales_amount"),
        F.sum("total_sales_num").alias("total_sales_num")
    )
except Exception as e:
    print(f"读取分类销售数据出错: {e}")
    category_sale = spark.sql(f"""
        SELECT 
            CAST(category_id AS INT) as category_id,
            category_name,
            SUM(total_sales_amount) as total_sales_amount,
            SUM(total_sales_num) as total_sales_num
        FROM gd02.dws_product_sale_summary 
        WHERE stat_date = '{process_date_ymd}' AND stat_period = 'day'
        GROUP BY category_id, category_name
    """)

# 计算总销售额（用于计算贡献占比）
total_sales_row = category_sale.agg(F.sum("total_sales_amount").alias("grand_total_sales")).collect()
if total_sales_row and len(total_sales_row) > 0:
    total_sales = total_sales_row[0][0]
else:
    total_sales = 0

# 计算排名和贡献占比
if total_sales and total_sales > 0:
    category_ranking = category_sale \
        .withColumn("sales_rank", F.row_number().over(
        Window.orderBy(F.col("total_sales_amount").desc())
    )) \
        .withColumn("sales_contribution", F.round(
        (F.col("total_sales_amount") / total_sales) * 100, 2
    )) \
        .withColumn("stat_date", F.lit(process_date_ymd)) \
        .withColumn("stat_period", F.lit("day")) \
        .withColumn("sales_rank", F.col("sales_rank").cast("int")) \
        .withColumn("total_sales_num", F.col("total_sales_num").cast("bigint"))
else:
    category_ranking = category_sale \
        .withColumn("sales_rank", F.lit(0)) \
        .withColumn("sales_contribution", F.lit(0.0)) \
        .withColumn("stat_date", F.lit(process_date_ymd)) \
        .withColumn("stat_period", F.lit("day")) \
        .withColumn("sales_rank", F.col("sales_rank").cast("int")) \
        .withColumn("total_sales_num", F.col("total_sales_num").cast("bigint"))

print_data_count(category_ranking, "ads_category_sale_ranking")

# 写入数据
category_ranking.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/ads/ads_category_sale_ranking")

repair_hive_table("ads_category_sale_ranking")


# ====================== 热门商品排行表 ads_hot_product_ranking ======================
create_hdfs_dir("/warehouse/gd02/ads/ads_hot_product_ranking")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.ads_hot_product_ranking (
    product_id INT COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id INT COMMENT '分类ID',
    total_sales_amount DOUBLE COMMENT '总销售额',
    total_visit_num INT COMMENT '总访问次数',
    conversion_rate DOUBLE COMMENT '转化率(%)',
    hot_rank INT COMMENT '热度排名',
    stat_date STRING COMMENT '统计日期'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/ads/ads_hot_product_ranking'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 关联销售和访问数据，处理数据类型转换
try:
    product_sale = spark.read.option("mergeSchema", "false").table("gd02.dws_product_sale_summary") \
        .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
        .select(
        F.col("product_id").cast("int").alias("product_id"),
        F.col("product_name"),
        F.col("category_id").cast("int").alias("category_id"),
        F.col("total_sales_amount").cast("double").alias("total_sales_amount")
    )
except Exception as e:
    print(f"读取商品销售数据出错: {e}")
    product_sale = spark.sql(f"""
        SELECT 
            CAST(product_id AS INT) as product_id,
            product_name,
            CAST(category_id AS INT) as category_id,
            SUM(total_sales_amount) as total_sales_amount
        FROM gd02.dws_product_sale_summary 
        WHERE stat_date = '{process_date_ymd}' AND stat_period = 'day'
        GROUP BY product_id, product_name, category_id
    """)

try:
    product_visit = spark.read.option("mergeSchema", "false").table("gd02.dws_product_visit_summary") \
        .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
        .select(
        F.col("product_id").cast("int").alias("product_id"),
        F.col("total_visit_num").cast("int").alias("total_visit_num"),
        F.col("pay_conversion_rate").cast("double").alias("pay_conversion_rate")
    )
except Exception as e:
    print(f"读取商品访问数据出错: {e}")
    product_visit = spark.sql(f"""
        SELECT 
            CAST(product_id AS INT) as product_id,
            SUM(total_visit_num) as total_visit_num,
            AVG(pay_conversion_rate) as pay_conversion_rate
        FROM gd02.dws_product_visit_summary 
        WHERE stat_date = '{process_date_ymd}' AND stat_period = 'day'
        GROUP BY product_id
    """)

# 计算热度排名（综合销量和访问量）
hot_product = product_sale.join(product_visit, on="product_id", how="outer") \
    .fillna(0) \
    .withColumn("conversion_rate", F.col("pay_conversion_rate")) \
    .withColumn("hot_score",
                F.col("total_sales_amount") * 0.6 +  # 销售额权重60%
                F.col("total_visit_num") * 0.3 +     # 访问量权重30%
                F.col("conversion_rate") * 0.1       # 转化率权重10%
                ) \
    .withColumn("hot_rank", F.row_number().over(
    Window.orderBy(F.col("hot_score").desc())
)) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day")) \
    .select(
    "product_id", "product_name", "category_id", "total_sales_amount",
    "total_visit_num", "conversion_rate", "hot_rank", "stat_date", "stat_period"
) \
    .withColumn("product_id", F.col("product_id").cast("int")) \
    .withColumn("category_id", F.col("category_id").cast("int")) \
    .withColumn("total_visit_num", F.col("total_visit_num").cast("int")) \
    .withColumn("hot_rank", F.col("hot_rank").cast("int"))

print_data_count(hot_product, "ads_hot_product_ranking")

# 写入数据
hot_product.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/ads/ads_hot_product_ranking")

repair_hive_table("ads_hot_product_ranking")


# ====================== 热门搜索词表 ads_hot_search_word ======================
create_hdfs_dir("/warehouse/gd02/ads/ads_hot_search_word")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.ads_hot_search_word (
    search_word STRING COMMENT '搜索词',
    total_search_num INT COMMENT '总搜索次数',
    click_rate DOUBLE COMMENT '点击转化率(%)',
    search_rank INT COMMENT '搜索排名',
    related_hot_product_ids ARRAY<INT> COMMENT '相关热门商品ID',
    stat_date STRING COMMENT '统计日期'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/ads/ads_hot_search_word'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 搜索词基础数据，处理数据类型转换
try:
    search_base = spark.read.option("mergeSchema", "false").table("gd02.dws_search_word_summary") \
        .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day")) \
        .select(
        F.col("search_word"),
        F.col("total_search_num").cast("int").alias("total_search_num"),
        F.col("related_product_ids")
    )
except Exception as e:
    print(f"读取搜索词基础数据出错: {e}")
    search_base = spark.sql(f"""
        SELECT 
            search_word,
            CAST(total_search_num AS INT) as total_search_num,
            related_product_ids
        FROM gd02.dws_search_word_summary 
        WHERE stat_date = '{process_date_ymd}' AND stat_period = 'day'
    """)

# 计算搜索点击转化率（搜索后购买的比例）
search_purchase = spark.table("gd02.dwd_product_search_detail") \
    .filter(F.col("dt") == process_date) \
    .join(
    spark.table("gd02.dwd_product_sale_detail").filter(F.col("dt") == process_date),
    on=["user_id", "product_id"],
    how="left"
) \
    .groupBy("search_word") \
    .agg(
    F.count("*").alias("total_search"),
    F.countDistinct("order_id").alias("purchase_count")
) \
    .withColumn("click_rate", F.round(
    (F.col("purchase_count") / F.col("total_search")) * 100, 2
)) \
    .select("search_word", "click_rate")

# 关联热门商品
try:
    hot_products = spark.read.option("mergeSchema", "false").table("gd02.ads_hot_product_ranking") \
        .filter((F.col("stat_date") == process_date_ymd) & (F.col("stat_period") == "day") & (F.col("hot_rank") <= 10)) \
        .select("product_id") \
        .agg(F.collect_list("product_id").alias("related_hot_product_ids"))
except Exception as e:
    print(f"读取热门商品数据出错: {e}")
    hot_products = spark.sql(f"""
        SELECT 
            COLLECT_LIST(CAST(product_id AS INT)) as related_hot_product_ids
        FROM gd02.ads_hot_product_ranking 
        WHERE stat_date = '{process_date_ymd}' AND stat_period = 'day' AND hot_rank <= 10
    """)

# 计算搜索排名
hot_search = search_base.join(search_purchase, on="search_word", how="left") \
    .crossJoin(hot_products) \
    .fillna(0) \
    .withColumn("search_score",
                F.col("total_search_num") * 0.7 +  # 搜索次数权重70%
                F.col("click_rate") * 0.3          # 转化率权重30%
                ) \
    .withColumn("search_rank", F.row_number().over(
    Window.orderBy(F.col("search_score").desc())
)) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day")) \
    .select(
    "search_word", "total_search_num", "click_rate", "search_rank",
    "related_hot_product_ids", "stat_date", "stat_period"
) \
    .withColumn("total_search_num", F.col("total_search_num").cast("int")) \
    .withColumn("search_rank", F.col("search_rank").cast("int"))

print_data_count(hot_search, "ads_hot_search_word")

# 写入数据
hot_search.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/ads/ads_hot_search_word")

repair_hive_table("ads_hot_search_word")


print("所有ADS层表创建及数据导入完成！")
spark.stop()
