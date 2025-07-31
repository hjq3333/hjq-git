from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import
from datetime import datetime, timedelta

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


# ====================== 关键步骤：清理历史数据避免冲突 ======================
# 先删除表和数据，确保全新开始
spark.sql("DROP TABLE IF EXISTS work_order.ads_wireless_entry_indicator")
delete_hdfs_path("/warehouse/work_order/ads/ads_wireless_entry_indicator")


# ====================== ADS层表创建 ======================
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


# ====================== 核心逻辑：合并店铺页细分类型 ======================
process_date = "20250707"
end_date = datetime.strptime(process_date, "%Y%m%d").date()

# 定义店铺页包含的细分类型
shop_page_subtypes = ["首页", "活动页", "分类页", "宝贝页", "新品页"]

# 1. 单日数据处理
day_data = spark.table("work_order.dwd_wireless_entry_detail").filter(
    F.col("dt") == process_date
).withColumn(
    # 合并页面类型：将细分类型归类为三大类
    "page_type",
    F.when(
        F.col("page_type").isin(shop_page_subtypes),  # 属于店铺页的细分类型
        F.lit("店铺页")
    ).when(
        F.col("page_type") == "商品详情页",  # 商品详情页保持不变
        F.lit("商品详情页")
    ).otherwise(
        F.lit("店铺其他页")  # 其他类型归为店铺其他页
    )
).groupBy("page_type") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),  # 合并后的访客数
    F.countDistinct(F.when(F.col("is_order") == 1, F.col("visitor_id"))).alias("order_buyer_count")  # 合并后的下单买家数
) \
    .withColumn("stat_period", F.lit("day")) \
    .withColumn("end_date", F.lit(end_date)) \
    .withColumn("dt", F.lit(process_date))

# 2. 7天数据处理（20250701-20250707）
start_date_7d = (end_date - timedelta(days=6)).strftime("%Y%m%d")
seven_day_data = spark.table("work_order.dwd_wireless_entry_detail").filter(
    F.col("dt").between(start_date_7d, process_date)
).withColumn(
    "page_type",  # 应用相同的类型合并逻辑
    F.when(
        F.col("page_type").isin(shop_page_subtypes),
        F.lit("店铺页")
    ).when(
        F.col("page_type") == "商品详情页",
        F.lit("商品详情页")
    ).otherwise(
        F.lit("店铺其他页")
    )
).groupBy("page_type") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),
    F.countDistinct(F.when(F.col("is_order") == 1, F.col("visitor_id"))).alias("order_buyer_count")
) \
    .withColumn("stat_period", F.lit("7day")) \
    .withColumn("end_date", F.lit(end_date)) \
    .withColumn("dt", F.lit(process_date))

# 3. 30天数据处理（20250608-20250707）
start_date_30d = (end_date - timedelta(days=29)).strftime("%Y%m%d")
thirty_day_data = spark.table("work_order.dwd_wireless_entry_detail").filter(
    F.col("dt").between(start_date_30d, process_date)
).withColumn(
    "page_type",  # 应用相同的类型合并逻辑
    F.when(
        F.col("page_type").isin(shop_page_subtypes),
        F.lit("店铺页")
    ).when(
        F.col("page_type") == "商品详情页",
        F.lit("商品详情页")
    ).otherwise(
        F.lit("店铺其他页")
    )
).groupBy("page_type") \
    .agg(
    F.countDistinct("visitor_id").alias("visitor_count"),
    F.countDistinct(F.when(F.col("is_order") == 1, F.col("visitor_id"))).alias("order_buyer_count")
) \
    .withColumn("stat_period", F.lit("30day")) \
    .withColumn("end_date", F.lit(end_date)) \
    .withColumn("dt", F.lit(process_date))

# 合并所有周期数据并计算转化率
all_period_data = day_data.unionByName(seven_day_data).unionByName(thirty_day_data)

ads_data = all_period_data.withColumn(
    "page_type_desc",  # 补充页面类型描述
    F.when(F.col("page_type") == "店铺页", "包含首页、活动页、分类页、宝贝页、新品页等")
    .when(F.col("page_type") == "商品详情页", "指商品的基础详情页面")
    .when(F.col("page_type") == "店铺其他页", "不属于前两类的兜底页面，如订阅页、直播页等")
    .otherwise("未知页面类型")
).withColumn(
    "conversion_rate_pct",  # 计算转化率（百分比）- 关键修改
    F.when(
        F.col("visitor_count") > 0,
        # 直接保留Double类型，与表结构一致
        F.round(F.col("order_buyer_count") / F.col("visitor_count") * 100, 2)
    ).otherwise(F.lit(0.0))  # 明确返回Double类型的0.0
).select(
    "page_type",
    "page_type_desc",
    "stat_period",
    "end_date",
    "visitor_count",
    "order_buyer_count",
    "conversion_rate_pct",
    "dt"
)

# 验证数据类型
print("转化率字段类型验证：")
ads_data.select("conversion_rate_pct").printSchema()

# 验证数据（确保只存在3种页面类型）
print_data_count(ads_data, "ads_wireless_entry_indicator")
print("\n修正后的页面类型分布：")
ads_data.groupBy("page_type").count().show()

# 写入ADS表 - 增加兼容性配置
ads_data.write.mode("overwrite") \
    .option("parquet.writeLegacyFormat", "true") \
    .parquet(f"/warehouse/work_order/ads/ads_wireless_entry_indicator/dt={process_date}")


# 修复分区
repair_hive_table("ads_wireless_entry_indicator")

# 最终验证：读取写入的数据
try:
    verify_df = spark.read.parquet(f"/warehouse/work_order/ads/ads_wireless_entry_indicator/dt={process_date}")
    print("\n写入后的数据验证：")
    verify_df.show()
except Exception as e:
    print(f"\n数据写入后验证失败：{e}")


