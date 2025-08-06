from pyspark.sql import SparkSession
import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Ecommerce_DWS_Tables") \
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

def force_delete_hdfs_path(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if fs.exists(jvm_path):
        fs.delete(jvm_path, True)
        print(f"已强制删除HDFS路径及所有内容：{path}")
    else:
        print(f"HDFS路径不存在：{path}")


# 处理日期参数
process_date = "20250101"  # 处理日期
process_date_ymd = datetime.datetime.strptime(process_date, "%Y%m%d").strftime("%Y-%m-%d")

# 计算7天前日期（用于周维度统计）
seven_days_ago = (datetime.datetime.strptime(process_date, "%Y%m%d") - datetime.timedelta(days=7)).strftime("%Y%m%d")


# ====================== 商品销售汇总表 dws_product_sale_summary ======================
create_hdfs_dir("/warehouse/gd02/dws/dws_product_sale_summary")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.dws_product_sale_summary (
    product_id INT COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id INT COMMENT '分类ID',
    category_name STRING COMMENT '分类名称',
    total_sales_amount DECIMAL(12,2) COMMENT '总销售额',
    total_sales_num INT COMMENT '总销量',
    total_order_num INT COMMENT '总订单数',
    avg_price DECIMAL(10,2) COMMENT '平均客单价',
    stat_date STRING COMMENT '统计日期'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/dws/dws_product_sale_summary'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 日维度销售汇总
daily_sale_summary = spark.table("gd02.dwd_product_sale_detail").alias("sale") \
    .filter(F.col("dt") == process_date) \
    .join(
    spark.table("gd02.ods_product_info").alias("product")
    .filter(F.col("dt") == process_date),
    on="product_id",
    how="inner"
) \
    .groupBy(
    F.col("sale.product_id"),
    F.col("product.product_name"),
    F.col("sale.category_id"),
    F.col("sale.category_name")
) \
    .agg(
    F.sum("pay_amount").cast("decimal(12,2)").alias("total_sales_amount"),
    F.sum("pay_num").alias("total_sales_num"),
    F.countDistinct("order_id").alias("total_order_num"),
    F.avg("pay_amount").cast("decimal(10,2)").alias("avg_price")
) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day"))

print_data_count(daily_sale_summary, "dws_product_sale_summary(daily)")

# 写入日维度数据
daily_sale_summary.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/dws/dws_product_sale_summary")

repair_hive_table("dws_product_sale_summary")


# ====================== 商品访问汇总表 dws_product_visit_summary ======================
create_hdfs_dir("/warehouse/gd02/dws/dws_product_visit_summary")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.dws_product_visit_summary (
    product_id INT COMMENT '商品ID',
    category_id INT COMMENT '分类ID',
    total_visitor_num INT COMMENT '总访客数',
    total_visit_num INT COMMENT '总访问次数',
    source_visitor_map MAP<STRING, INT> COMMENT '各来源访客数映射',
    pay_conversion_rate DECIMAL(5,2) COMMENT '支付转化率(%)',
    stat_date STRING COMMENT '统计日期'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/dws/dws_product_visit_summary'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 访问数据汇总
visit_summary = spark.table("gd02.dwd_product_visit_detail").alias("visit") \
    .filter(F.col("dt") == process_date) \
    .groupBy(
    F.col("product_id"),
    F.col("category_id")
) \
    .agg(
    F.countDistinct("user_id").alias("total_visitor_num"),
    F.count("*").alias("total_visit_num"),
    F.map_from_entries(
        F.collect_list(
            F.struct("source_name", F.countDistinct("user_id").alias("cnt"))
        )
    ).alias("source_visitor_map")
)

# 关联支付数据计算转化率
pay_conversion = spark.table("gd02.dwd_product_sale_detail").alias("sale") \
    .filter(F.col("dt") == process_date) \
    .groupBy("product_id") \
    .agg(
    F.countDistinct("user_id").alias("pay_user_num")
)

# 合并计算转化率
final_visit_summary = visit_summary.join(
    pay_conversion,
    on="product_id",
    how="left"
).withColumn(
    "pay_conversion_rate",
    F.round(
        (F.col("pay_user_num") / F.col("total_visitor_num")) * 100,
        2
    ).cast("decimal(5,2)")
).withColumn(
    "stat_date", F.lit(process_date_ymd)
).withColumn(
    "stat_period", F.lit("day")
).fillna(0, subset=["pay_conversion_rate", "pay_user_num"])

print_data_count(final_visit_summary, "dws_product_visit_summary")

# 写入数据
final_visit_summary.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/dws/dws_product_visit_summary")

repair_hive_table("dws_product_visit_summary")


# ====================== SKU销售汇总表 dws_sku_sale_summary ======================
create_hdfs_dir("/warehouse/gd02/dws/dws_sku_sale_summary")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.dws_sku_sale_summary (
    sku_id INT COMMENT 'SKU ID',
    product_id INT COMMENT '商品ID',
    total_pay_num INT COMMENT '总支付件数',
    pay_num_ratio DECIMAL(5,2) COMMENT '支付件数占比(%)',
    current_stock INT COMMENT '当前库存',
    stock_sale_days INT COMMENT '库存可售天数',
    stat_date STRING COMMENT '统计日期'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/dws/dws_sku_sale_summary'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 计算SKU销售占比
sku_sale = spark.table("gd02.dwd_product_sale_detail").alias("sale") \
    .filter(F.col("dt") == process_date) \
    .groupBy("sku_id", "product_id") \
    .agg(
    F.sum("pay_num").alias("total_pay_num")
)

# 计算商品总销量（用于计算占比）
product_total = sku_sale.groupBy("product_id") \
    .agg(
    F.sum("total_pay_num").alias("product_total_pay_num")
)

# 关联库存数据
sku_stock = spark.table("gd02.ods_product_info").alias("product") \
    .filter(F.col("dt") == process_date) \
    .select(
    F.col("product_id"),
    F.col("stock_num").alias("current_stock")
)

# 合并计算
final_sku_summary = sku_sale.join(
    product_total,
    on="product_id",
    how="inner"
).join(
    sku_stock,
    on="product_id",
    how="left"
).withColumn(
    "pay_num_ratio",
    F.round(
        (F.col("total_pay_num") / F.col("product_total_pay_num")) * 100,
        2
    ).cast("decimal(5,2)")
).withColumn(
    # 计算库存可售天数（当前库存/日均销量）
    "stock_sale_days",
    F.when(
        F.col("total_pay_num") > 0,
        F.floor(F.col("current_stock") / F.col("total_pay_num"))
    ).otherwise(F.lit(999))  # 销量为0时设为999天
).withColumn(
    "stat_date", F.lit(process_date_ymd)
).withColumn(
    "stat_period", F.lit("day")
).select(
    "sku_id", "product_id", "total_pay_num", "pay_num_ratio",
    "current_stock", "stock_sale_days", "stat_date", "stat_period"
)

print_data_count(final_sku_summary, "dws_sku_sale_summary")

# 写入数据
final_sku_summary.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/dws/dws_sku_sale_summary")

repair_hive_table("dws_sku_sale_summary")


# ====================== 搜索词汇总表 dws_search_word_summary ======================
create_hdfs_dir("/warehouse/gd02/dws/dws_search_word_summary")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.dws_search_word_summary (
    search_word STRING COMMENT '搜索词',
    total_search_num INT COMMENT '总搜索次数',
    total_visitor_num INT COMMENT '搜索用户数',
    related_product_ids ARRAY<INT> COMMENT '相关商品ID数组',
    stat_date STRING COMMENT '统计日期'
) 
PARTITIONED BY (stat_period STRING COMMENT '统计周期（day/week/month）')
STORED AS PARQUET
LOCATION '/warehouse/gd02/dws/dws_search_word_summary'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 搜索词汇总
search_summary = spark.table("gd02.dwd_product_search_detail").alias("search") \
    .filter(F.col("dt") == process_date) \
    .groupBy("search_word") \
    .agg(
    F.count("*").alias("total_search_num"),
    F.countDistinct("user_id").alias("total_visitor_num"),
    F.collect_set("product_id").alias("related_product_ids")
) \
    .withColumn("stat_date", F.lit(process_date_ymd)) \
    .withColumn("stat_period", F.lit("day"))

print_data_count(search_summary, "dws_search_word_summary")

# 写入数据
search_summary.write.mode("append") \
    .partitionBy("stat_period") \
    .parquet("/warehouse/gd02/dws/dws_search_word_summary")

repair_hive_table("dws_search_word_summary")


print("所有DWS层表创建及数据导入完成！")
spark.stop()
    