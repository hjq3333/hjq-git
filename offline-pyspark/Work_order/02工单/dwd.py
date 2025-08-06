from pyspark.sql import SparkSession
import datetime
from pyspark.sql import functions as F
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Ecommerce_DWD_Tables") \
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
process_date = "20250108"  # 可根据需要修改为具体日期
process_date_ymd = datetime.datetime.strptime(process_date, "%Y%m%d").strftime("%Y-%m-%d")


# ====================== 商品访问明细事实表 dwd_product_visit_detail ======================
create_hdfs_dir("/warehouse/gd02/dwd/dwd_product_visit_detail")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.dwd_product_visit_detail (
    user_id INT COMMENT '用户ID',
    product_id INT COMMENT '商品ID',
    category_id INT COMMENT '分类ID',
    visit_time TIMESTAMP COMMENT '访问时间',
    source_id STRING COMMENT '流量来源ID',
    source_name STRING COMMENT '流量来源名称',
    source_type STRING COMMENT '流量来源类型',
    stay_time INT COMMENT '停留时间（秒）'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd02/dwd/dwd_product_visit_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 从ODS层和DIM层关联清洗数据
visit_detail_data = spark.table("gd02.ods_shop_visit_log").alias("visit") \
    .filter(F.col("dt") == process_date) \
    .join(
    spark.table("gd02.ods_product_info").alias("product")
    .filter(F.col("dt") == process_date),
    on="product_id",
    how="inner"
) \
    .join(
    spark.table("gd02.dim_traffic_source").alias("source"),
    on=F.col("visit.visit_source") == F.col("source.source_name"),
    how="left"
) \
    .select(
    F.col("visit.user_id").cast("int").alias("user_id"),
    F.col("visit.product_id").cast("int").alias("product_id"),
    F.col("product.category_id").cast("int").alias("category_id"),
    F.col("visit.visit_time").alias("visit_time"),
    F.md5(F.col("source.source_name")).alias("source_id"),  # 生成来源ID
    F.col("source.source_name").alias("source_name"),
    F.col("source.source_type").alias("source_type"),
    F.lit(60).alias("stay_time")  # 模拟停留时间
) \
    .filter(
    (F.col("user_id").isNotNull()) &
    (F.col("product_id").isNotNull()) &
    (F.col("visit_time").isNotNull())
) \
    .dropDuplicates(["user_id", "product_id", "visit_time"])

# 添加分区列
visit_detail_data = visit_detail_data.withColumn("dt", F.lit(process_date))

print_data_count(visit_detail_data, "dwd_product_visit_detail")

# 写入数据
visit_detail_data.write.mode("append") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd02/dwd/dwd_product_visit_detail")

repair_hive_table("dwd_product_visit_detail")


# ====================== 商品销售明细事实表 dwd_product_sale_detail ======================
create_hdfs_dir("/warehouse/gd02/dwd/dwd_product_sale_detail")

# 在读取表时添加选项
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")


spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.dwd_product_sale_detail (
    order_id STRING COMMENT '订单ID',
    product_id INT COMMENT '商品ID',
    sku_id INT COMMENT 'SKU ID',
    user_id INT COMMENT '用户ID',
    pay_amount DOUBLE COMMENT '支付金额',
    pay_num INT COMMENT '支付件数',
    pay_time TIMESTAMP COMMENT '支付时间',
    category_id INT COMMENT '商品分类ID',
    category_name STRING COMMENT '商品分类名称'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd02/dwd/dwd_product_sale_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 从ODS层和DIM层关联清洗数据
sale_detail_data = spark.table("gd02.ods_product_sale_info").alias("sale") \
    .filter(F.col("dt") == process_date) \
    .join(
    spark.table("gd02.dim_product_category").alias("category"),
    on="category_id",
    how="left"
) \
    .select(
    F.col("sale.order_id").alias("order_id"),
    F.col("sale.product_id").cast("int").alias("product_id"),
    F.col("sale.sku_id").cast("int").alias("sku_id"),
    F.col("sale.user_id").cast("int").alias("user_id"),
    F.col("sale.pay_amount").cast("DOUBLE").alias("pay_amount"),
    F.col("sale.pay_num").cast("int").alias("pay_num"),
    F.col("sale.pay_time").alias("pay_time"),
    F.col("sale.category_id").cast("int").alias("category_id"),
    F.col("category.category_name").alias("category_name")
) \
    .filter(
    (F.col("order_id").isNotNull()) &
    (F.col("product_id").isNotNull()) &
    (F.col("pay_amount") > 0) &  # 过滤有效订单
    (F.col("pay_num") > 0)
) \
    .dropDuplicates(["order_id", "sku_id"])

# 添加分区列
sale_detail_data = sale_detail_data.withColumn("dt", F.lit(process_date))

print_data_count(sale_detail_data, "dwd_product_sale_detail")

# 写入数据
sale_detail_data.write.mode("append") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd02/dwd/dwd_product_sale_detail")

repair_hive_table("dwd_product_sale_detail")


# ====================== 商品搜索明细事实表 dwd_product_search_detail ======================
create_hdfs_dir("/warehouse/gd02/dwd/dwd_product_search_detail")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gd02.dwd_product_search_detail (
    user_id INT COMMENT '用户ID',
    search_word STRING COMMENT '搜索词',
    product_id INT COMMENT '商品ID',
    search_time TIMESTAMP COMMENT '搜索时间',
    category_id INT COMMENT '商品分类ID',
    category_name STRING COMMENT '商品分类名称'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd02/dwd/dwd_product_search_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 从ODS层和DIM层关联清洗数据
search_detail_data = spark.table("gd02.ods_search_log").alias("search") \
    .filter(F.col("dt") == process_date) \
    .join(
    spark.table("gd02.ods_product_info").alias("product")
    .filter(F.col("dt") == process_date),
    on="product_id",
    how="inner"
) \
    .join(
    spark.table("gd02.dim_product_category").alias("category"),
    on="category_id",
    how="left"
) \
    .select(
    F.col("search.user_id").cast("int").alias("user_id"),
    F.trim(F.col("search.search_word")).alias("search_word"),  # 去除空格
    F.col("search.product_id").cast("int").alias("product_id"),
    F.col("search.search_time").alias("search_time"),
    F.col("product.category_id").cast("int").alias("category_id"),
    F.col("category.category_name").alias("category_name")
) \
    .filter(
    (F.col("user_id").isNotNull()) &
    (F.col("search_word").isNotNull()) &
    (F.col("search_time").isNotNull())
) \
    .dropDuplicates(["user_id", "search_word", "search_time"])

# 添加分区列
search_detail_data = search_detail_data.withColumn("dt", F.lit(process_date))

print_data_count(search_detail_data, "dwd_product_search_detail")

# 写入数据
search_detail_data.write.mode("append") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd02/dwd/dwd_product_search_detail")

repair_hive_table("dwd_product_search_detail")


print("所有DWD层表创建及数据导入完成！")
spark.stop()
