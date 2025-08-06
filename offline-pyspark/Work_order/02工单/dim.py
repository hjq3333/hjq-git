from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("Ecommerce_ODS_DIM_Tables") \
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

# 使用指定数据库
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
    """打印数据量"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count


# ====================== DIM层表 ======================

# 1. 商品分类维度表 dim_product_category
create_hdfs_dir("/warehouse/gd02/dim/dim_product_category")
spark.sql("DROP TABLE IF EXISTS gd02.dim_product_category")
spark.sql("""
CREATE EXTERNAL TABLE gd02.dim_product_category (
    category_id INT COMMENT '分类ID',
    category_name STRING COMMENT '分类名称',
    parent_category_id INT COMMENT '父分类ID'
) 
STORED AS ORC
LOCATION '/warehouse/gd02/dim/dim_product_category'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 读取CSV并写入
dim_category_csv = "hdfs://cdh01:8020/data/dim_product_category.csv"
dim_category_df = spark.read.csv(
    dim_category_csv,
    header=True,
    sep=',',
    inferSchema=True
)
print_data_count(dim_category_df, "dim_product_category")

dim_category_df.write.mode("overwrite") \
    .orc("/warehouse/gd02/dim/dim_product_category")


# 2. 流量来源维度表 dim_traffic_source
create_hdfs_dir("/warehouse/gd02/dim/dim_traffic_source")
spark.sql("DROP TABLE IF EXISTS gd02.dim_traffic_source")
spark.sql("""
CREATE EXTERNAL TABLE gd02.dim_traffic_source (
    source_name STRING COMMENT '来源名称',
    source_type STRING COMMENT '来源类型（如广告/自然搜索）'
) 
STORED AS ORC
LOCATION '/warehouse/gd02/dim/dim_traffic_source'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 读取CSV并写入
dim_traffic_csv = "hdfs://cdh01:8020/data/dim_traffic_source.csv"
dim_traffic_df = spark.read.csv(
    dim_traffic_csv,
    header=True,
    sep=',',
    inferSchema=True
)
print_data_count(dim_traffic_df, "dim_traffic_source")

dim_traffic_df.write.mode("overwrite") \
    .orc("/warehouse/gd02/dim/dim_traffic_source")

print("所有DIM表创建及数据导入完成！")
spark.stop()