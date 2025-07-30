from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("WirelessEntryDataProcessing") \
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

# 设置Hive数据库
spark.sql("USE tms01")

def create_hdfs_dir(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

# 修复Hive表分区的函数
def repair_hive_table(table_name):
    spark.sql(f"MSCK REPAIR TABLE work_order.{table_name}")
    print(f"修复分区完成：work_order.{table_name}")

# 打印数据量的函数
def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# 创建HDFS目录
create_hdfs_dir("/warehouse/work_order/ods/ods_wireless_entry")

# 删除已存在的表
spark.sql("DROP TABLE IF EXISTS work_order.ods_wireless_entry")

# 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS work_order.ods_wireless_entry (
    page_id STRING COMMENT '页面唯一标识',
    page_type STRING COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
    visitor_id STRING COMMENT '访客唯一标识',
    visit_time TIMESTAMP COMMENT '访问时间（精确到秒）',
    is_order TINYINT COMMENT '是否下单（1=是，0=否）',
    data_date DATE COMMENT '数据日期'
) 
COMMENT '无线端入店与承接原始数据，存储访客访问及下单行为明细'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '/warehouse/work_order/ods/ods_wireless_entry'
""")

# CSV文件路径，请替换为实际路径
csv_path = "hdfs://cdh01:8020/data/ods_wireless_entry.csv"

# 定义 schema
schema = StructType([
    StructField("page_id", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("visitor_id", StringType(), True),
    StructField("visit_time", TimestampType(), True),
    StructField("is_order", IntegerType(), True),
    StructField("data_date", DateType(), True)
])

# 用指定的 schema 读取 CSV
wireless_entry_df = spark.read.csv(
    csv_path,
    header=True,
    sep="\t",
    schema=schema,
    timestampFormat="yyyy-MM-dd HH:mm:ss"
)

# 添加分区列
wireless_entry_df = wireless_entry_df.withColumn(
    "dt",
    F.date_format(F.col("data_date"), "yyyyMMdd")
)

# 验证数据量
print_data_count(wireless_entry_df, "ods_wireless_entry")

# 写入数据到HDFS
wireless_entry_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .option("sep", "\t") \
    .csv("/warehouse/work_order/ods/ods_wireless_entry")

# 修复分区
repair_hive_table("ods_wireless_entry")

# 验证数据
print("验证数据是否成功加载到Hive表:")
spark.table("work_order.ods_wireless_entry").show(5)

# 停止SparkSession
spark.stop()