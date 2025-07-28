from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("wl_DWS_Aggregation") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()

# 导入Hadoop文件系统类
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())



# 辅助函数定义
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
    spark.sql(f"MSCK REPAIR TABLE tms_spark_dws.{table_name}")
    print(f"修复分区完成：tms_spark_dws.{table_name}")

def print_data_count(df, table_name):
    """打印DataFrame数据量"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# ====================== 1. dws_trade_org_cargo_type_order_1d ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trade_org_cargo_type_order_1d")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trade_org_cargo_type_order_1d")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trade_org_cargo_type_order_1d (
    org_id          bigint comment '机构ID',
    org_name        string comment '转运站名称',
    city_id         bigint comment '城市ID',
    city_name       string comment '城市名称',
    cargo_type      string comment '货物类型',
    cargo_type_name string comment '货物类型名称',
    order_count     bigint comment '下单数',
    order_amount    decimal(16, 2) comment '下单金额'
)
comment '交易域机构货物类型粒度下单1日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trade_org_cargo_type_order_1d'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

# 数据处理
detail = spark.table("tms_spark_dwd.dwd_trade_order_detail_inc") \
    .select("order_id", "cargo_type", "cargo_type_name", "sender_district_id", "sender_city_id", "amount", "dt")

distinct_detail = detail.groupBy(
    "order_id", "cargo_type", "cargo_type_name", "sender_district_id", "sender_city_id", "dt"
).agg(F.max("amount").alias("amount"))

org = spark.table("tms_spark_dim.dim_organ_full").filter(F.col("dt") == "20250713") \
    .select("id", "org_name", "region_id").withColumnRenamed("id", "org_id")

agg_data = distinct_detail.join(
    org,
    distinct_detail.sender_district_id == org.region_id,
    "left"
).groupBy(
    "org_id", "org_name", "cargo_type", "cargo_type_name", "sender_city_id", "dt"
).agg(
    F.count("order_id").alias("order_count"),
    F.sum("amount").alias("order_amount")
).withColumnRenamed("sender_city_id", "city_id")

region = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name").withColumnRenamed("name", "city_name")

final_df = agg_data.join(
    region,
    agg_data.city_id == region.id,
    "left"
).select(
    "org_id", "org_name", "city_id", "city_name",
    "cargo_type", "cargo_type_name", "order_count", "order_amount", "dt"
)

print_data_count(final_df, "dws_trade_org_cargo_type_order_1d")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trade_org_cargo_type_order_1d"
)
repair_hive_table("dws_trade_org_cargo_type_order_1d")

# ====================== 2. dws_trans_org_receive_1d ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_org_receive_1d/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_org_receive_1d")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_org_receive_1d (
    org_id        bigint comment '转运站ID',
    org_name      string comment '转运站名称',
    city_id       bigint comment '城市ID',
    city_name     string comment '城市名称',
    province_id   bigint comment '省份ID',
    province_name string comment '省份名称',
    order_count   bigint comment '揽收次数',
    order_amount  decimal(16, 2) comment '揽收金额'
)
comment '物流域转运站粒度揽收1日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_org_receive_1d/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

detail = spark.table("tms_spark_dwd.dwd_trans_receive_detail_inc") \
    .select("order_id", "amount", "sender_district_id", "dt")

organ = spark.table("tms_spark_dim.dim_organ_full").filter(F.col("dt") == "20250713") \
    .select("id", "org_name", "region_id").withColumnRenamed("id", "org_id")

district = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "parent_id")

city = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name", "parent_id").withColumnRenamed("id", "city_id").withColumnRenamed("name", "city_name")

province = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name").withColumnRenamed("id", "province_id").withColumnRenamed("name", "province_name")

distinct_tb = detail.join(organ, detail.sender_district_id == organ.region_id, "left") \
    .join(district, organ.region_id == district.id, "left") \
    .join(city, district.parent_id == city.city_id, "left") \
    .join(province, city.parent_id == province.province_id, "left") \
    .groupBy(
    "order_id", "org_id", "org_name", "city_id", "city_name",
    "province_id", "province_name", "dt"
).agg(F.max("amount").alias("distinct_amount"))

final_df = distinct_tb.groupBy(
    "org_id", "org_name", "city_id", "city_name",
    "province_id", "province_name", "dt"
).agg(
    F.count("order_id").alias("order_count"),
    F.sum("distinct_amount").alias("order_amount")
)

print_data_count(final_df, "dws_trans_org_receive_1d")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_org_receive_1d/"
)
repair_hive_table("dws_trans_org_receive_1d")

# ====================== 3. dws_trans_dispatch_1d ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_dispatch_1d/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_dispatch_1d")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_dispatch_1d (
    order_count  bigint comment '发单总数',
    order_amount decimal(16, 2) comment '发单总金额'
)
comment '物流域发单1日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_dispatch_1d/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

distinct_info = spark.table("tms_spark_dwd.dwd_trans_dispatch_detail_inc") \
    .groupBy("order_id", "dt") \
    .agg(F.max("amount").alias("distinct_amount"))

final_df = distinct_info.groupBy("dt").agg(
    F.count("order_id").alias("order_count"),
    F.sum("distinct_amount").alias("order_amount")
)

print_data_count(final_df, "dws_trans_dispatch_1d")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_dispatch_1d/"
)
repair_hive_table("dws_trans_dispatch_1d")

# ====================== 4. dws_trans_org_truck_model_type_trans_finish_1d ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_org_truck_model_type_trans_finish_1d/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_org_truck_model_type_trans_finish_1d")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_org_truck_model_type_trans_finish_1d (
    org_id                bigint comment '机构ID',
    org_name              string comment '机构名称',
    truck_model_type      string comment '卡车类别编码',
    truck_model_type_name string comment '卡车类别名称',
    trans_finish_count    bigint comment '运输完成次数',
    trans_finish_distance decimal(16, 2) comment '运输完成里程',
    trans_finish_dur_sec  bigint comment '运输完成时长，单位：秒'
)
comment '物流域机构卡车类别粒度运输最近1日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_org_truck_model_type_trans_finish_1d/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

trans_finish = spark.table("tms_spark_dwd.dwd_trans_trans_finish_inc") \
    .select("id", "start_org_id", "start_org_name", "truck_id",
            "actual_distance", "finish_dur_sec", "dt") \
    .withColumnRenamed("start_org_id", "org_id") \
    .withColumnRenamed("start_org_name", "org_name")

truck_info = spark.table("tms_spark_dim.dim_truck_full").filter(F.col("dt") == "20250713") \
    .select("id", "truck_model_type", "truck_model_type_name")

final_df = trans_finish.join(truck_info, trans_finish.truck_id == truck_info.id, "left") \
    .groupBy("org_id", "org_name", "truck_model_type", "truck_model_type_name", "dt") \
    .agg(
    F.count("dim_truck_full.id").alias("trans_finish_count"),
    F.sum("actual_distance").alias("trans_finish_distance"),
    F.sum("finish_dur_sec").alias("trans_finish_dur_sec")
)

print_data_count(final_df, "dws_trans_org_truck_model_type_trans_finish_1d")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_org_truck_model_type_trans_finish_1d/"
)
repair_hive_table("dws_trans_org_truck_model_type_trans_finish_1d")

# ====================== 5. dws_trans_org_deliver_suc_1d ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_org_deliver_suc_1d/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_org_deliver_suc_1d")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_org_deliver_suc_1d (
    org_id        bigint comment '转运站ID',
    org_name      string comment '转运站名称',
    city_id       bigint comment '城市ID',
    city_name     string comment '城市名称',
    province_id   bigint comment '省份ID',
    province_name string comment '省份名称',
    order_count   bigint comment '派送成功次数（订单数）'
)
comment '物流域转运站粒度派送成功1日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_org_deliver_suc_1d/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

detail = spark.table("tms_spark_dwd.dwd_trans_deliver_suc_detail_inc") \
    .groupBy("order_id", "receiver_district_id", "dt").count()

organ = spark.table("tms_spark_dim.dim_organ_full").filter(F.col("dt") == "20250713") \
    .select("id", "org_name", "region_id") \
    .withColumnRenamed("id", "org_id") \
    .withColumnRenamed("region_id", "district_id")

district = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "parent_id").withColumnRenamed("parent_id", "city_id")

city = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name", "parent_id").withColumnRenamed("parent_id", "province_id")

province = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name").withColumnRenamed("name", "province_name")

final_df = detail.join(organ, detail.receiver_district_id == organ.district_id, "left") \
    .join(district, organ.district_id == district.id, "left") \
    .join(city, district.city_id == city.id, "left") \
    .join(province, city.province_id == province.id, "left") \
    .groupBy(
    "org_id", "org_name", "city_id", city.name.alias("city_name"),
    "province_id", "province_name", "dt"
).agg(F.count("order_id").alias("order_count"))

print_data_count(final_df, "dws_trans_org_deliver_suc_1d")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_org_deliver_suc_1d/"
)
repair_hive_table("dws_trans_org_deliver_suc_1d")

# ====================== 6. dws_trans_org_sort_1d ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_org_sort_1d/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_org_sort_1d")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_org_sort_1d (
    org_id        bigint comment '机构ID',
    org_name      string comment '机构名称',
    city_id       bigint comment '城市ID',
    city_name     string comment '城市名称',
    province_id   bigint comment '省份ID',
    province_name string comment '省份名称',
    sort_count    bigint comment '分拣次数'
)
comment '物流域机构粒度分拣1日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_org_sort_1d/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

agg = spark.table("tms_spark_dwd.dwd_bound_sort_inc") \
    .groupBy("org_id", "dt") \
    .agg(F.count("*").alias("sort_count"))

org = spark.table("tms_spark_dim.dim_organ_full").filter(F.col("dt") == "20250713") \
    .select("id", "org_name", "org_level", "region_id").withColumnRenamed("id", "org_id")

city_for_level1 = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name", "parent_id")

province_for_level1 = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name", "parent_id")

province_for_level2 = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name")

final_df = agg.join(org, "org_id", "left") \
    .join(city_for_level1, org.region_id == city_for_level1.id, "left") \
    .join(province_for_level1, city_for_level1.parent_id == province_for_level1.id, "left") \
    .join(province_for_level2, province_for_level1.parent_id == province_for_level2.id, "left") \
    .select(
    "org_id", "org_name",
    F.when(org.org_level == 1, city_for_level1.id).otherwise(province_for_level1.id).alias("city_id"),
    F.when(org.org_level == 1, city_for_level1.name).otherwise(province_for_level1.name).alias("city_name"),
    F.when(org.org_level == 1, province_for_level1.id).otherwise(province_for_level2.id).alias("province_id"),
    F.when(org.org_level == 1, province_for_level1.name).otherwise(province_for_level2.name).alias("province_name"),
    "sort_count", "dt"
)

print_data_count(final_df, "dws_trans_org_sort_1d")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_org_sort_1d/"
)
repair_hive_table("dws_trans_org_sort_1d")

# ====================== 7. dws_trade_org_cargo_type_order_nd ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trade_org_cargo_type_order_nd")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trade_org_cargo_type_order_nd")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trade_org_cargo_type_order_nd (
    org_id          bigint comment '机构ID',
    org_name        string comment '转运站名称',
    city_id         bigint comment '城市ID',
    city_name       string comment '城市名称',
    cargo_type      string comment '货物类型',
    cargo_type_name string comment '货物类型名称',
    recent_days     tinyint comment '最近天数',
    order_count     bigint comment '下单数',
    order_amount    decimal(16, 2) comment '下单金额'
)
comment '交易域机构货物类型粒度下单n日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trade_org_cargo_type_order_nd'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

base_data = spark.table("tms_spark_dws.dws_trade_org_cargo_type_order_1d")
exploded = base_data.select(
    "*",
    F.explode(F.array(F.lit(7), F.lit(30))).alias("recent_days")
)

final_df = exploded.groupBy(
    "org_id", "org_name", "city_id", "city_name",
    "cargo_type", "cargo_type_name", "recent_days"
).agg(
    F.sum("order_count").alias("order_count"),
    F.sum("order_amount").alias("order_amount")
).withColumn("dt", F.lit("20250713"))

print_data_count(final_df, "dws_trade_org_cargo_type_order_nd")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trade_org_cargo_type_order_nd"
)
repair_hive_table("dws_trade_org_cargo_type_order_nd")

# ====================== 8. dws_trans_org_receive_nd ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_org_receive_nd/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_org_receive_nd")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_org_receive_nd (
    org_id        bigint comment '转运站ID',
    org_name      string comment '转运站名称',
    city_id       bigint comment '城市ID',
    city_name     string comment '城市名称',
    province_id   bigint comment '省份ID',
    province_name string comment '省份名称',
    recent_days   tinyint comment '最近天数',
    order_count   bigint comment '揽收次数',
    order_amount  decimal(16, 2) comment '揽收金额'
) 
comment '物流域转运站粒度揽收n日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_org_receive_nd/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

base_data = spark.table("tms_spark_dws.dws_trans_org_receive_1d")
# 1. 先通过explode生成recent_days列
exploded = base_data.select(
    "*",
    F.explode(F.array(F.lit(7), F.lit(30))).alias("recent_days")
)

# 2. 过滤数据（确保dt在recent_days范围内）
filtered = exploded.filter(
    F.col("dt") >= F.date_add(F.lit("2025-07-11"), F.col("recent_days") * -1 + 1)
)

# 3. 执行聚合操作（明确指定列的来源）
final_df = filtered.groupBy(
    "org_id", "org_name", "city_id", "city_name",
    "province_id", "province_name", "recent_days"
).agg(
    F.sum("order_count").alias("order_count"),
    F.sum("order_amount").alias("order_amount")
).withColumn("dt", F.lit("20250713"))

# 打印数据量并写入HDFS
print_data_count(final_df, "dws_trans_org_receive_nd")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_org_receive_nd/"
)
repair_hive_table("dws_trans_org_receive_nd")

# ====================== 9. dws_trans_dispatch_nd ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_dispatch_nd/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_dispatch_nd")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_dispatch_nd (
    recent_days  tinyint comment '最近天数',
    order_count  bigint comment '发单总数',
    order_amount decimal(16, 2) comment '发单总金额'
) 
comment '物流域发单1日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_dispatch_nd/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

base_data = spark.table("tms_spark_dws.dws_trans_dispatch_1d")
# 1. 先生成recent_days列（从数组炸裂）
exploded = base_data.select(
    "*",
    F.explode(F.array(F.lit(7), F.lit(30))).alias("recent_days")  # 先添加recent_days
)

# 2. 再用生成的recent_days进行过滤（此时列已存在）
filtered = exploded.filter(
    F.col("dt") >= F.date_add(F.lit("2025-07-11"), F.expr("-recent_days + 1"))
)

# 3. 基于过滤后的数据聚合
final_df = filtered.groupBy("recent_days").agg(
    F.sum("order_count").alias("order_count"),
    F.sum("order_amount").alias("order_amount")
).withColumn("dt", F.lit("20250713"))  # 添加分区日期

print_data_count(final_df, "dws_trans_dispatch_nd")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_dispatch_nd/"
)
repair_hive_table("dws_trans_dispatch_nd")

# ====================== 10. dws_trans_shift_trans_finish_nd ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_shift_trans_finish_nd/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_shift_trans_finish_nd")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_shift_trans_finish_nd (
    shift_id                 bigint comment '班次ID',
    city_id                  bigint comment '城市ID',
    city_name                string comment '城市名称',
    org_id                   bigint comment '机构ID',
    org_name                 string comment '机构名称',
    line_id                  bigint comment '线路ID',
    line_name                string comment '线路名称',
    driver1_emp_id           bigint comment '第一司机员工ID',
    driver1_name             string comment '第一司机姓名',
    driver2_emp_id           bigint comment '第二司机员工ID',
    driver2_name             string comment '第二司机姓名',
    truck_model_type         string comment '卡车类别编码',
    truck_model_type_name    string comment '卡车类别名称',
    recent_days              tinyint comment '最近天数',
    trans_finish_count       bigint comment '转运完成次数',
    trans_finish_distance    decimal(16, 2) comment '转运完成里程',
    trans_finish_dur_sec     bigint comment '转运完成时长，单位：秒',
    trans_finish_order_count bigint comment '转运完成运单数'
) 
comment '物流域班次粒度转运完成最近n日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_shift_trans_finish_nd/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

base_data = spark.table("tms_spark_dwd.dwd_trans_trans_finish_inc") \
    .select("id", "shift_id", "line_id", "start_org_id", "start_org_name",
            "driver1_emp_id", "driver1_name", "driver2_emp_id", "driver2_name",
            "truck_id", "actual_distance", "finish_dur_sec", "order_num", "dt")

aggregated = base_data.select(
    "*",
    F.explode(F.array(F.lit(7), F.lit(30))).alias("recent_days")
).groupBy(
    "recent_days", "shift_id", "line_id", "start_org_id", "start_org_name",
    "driver1_emp_id", "driver1_name", "driver2_emp_id", "driver2_name", "truck_id"
).agg(
    F.count("id").alias("trans_finish_count"),
    F.sum("actual_distance").alias("trans_finish_distance"),
    F.sum("finish_dur_sec").alias("trans_finish_dur_sec"),
    F.sum("order_num").alias("trans_finish_order_count")
).withColumnRenamed("start_org_id", "org_id") \
    .withColumnRenamed("start_org_name", "org_name")

org_info = spark.table("tms_spark_dim.dim_organ_full").filter(F.col("dt") == "20250713") \
    .select("id", "org_level", "region_id", "region_name").withColumnRenamed("id", "org_id")

parent = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "parent_id")

city = spark.table("tms_spark_dim.dim_region_full").filter(F.col("dt") == "20250713") \
    .select("id", "name")

for_line_name = spark.table("tms_spark_dim.dim_shift_full").filter(F.col("dt") == "20250713") \
    .select("id", "line_name")

truck_info = spark.table("tms_spark_dim.dim_truck_full").filter(F.col("dt") == "20250713") \
    .select("id", "truck_model_type", "truck_model_type_name")

final_df = aggregated.join(org_info, "org_id", "left") \
    .join(parent, org_info.region_id == parent.id, "left") \
    .join(city, parent.parent_id == city.id, "left") \
    .join(for_line_name, aggregated.shift_id == for_line_name.id, "left") \
    .join(truck_info, aggregated.truck_id == truck_info.id, "left") \
    .select(
    "shift_id",
    F.when(org_info.org_level == 1, org_info.region_id).otherwise(city.id).alias("city_id"),
    F.when(org_info.org_level == 1, org_info.region_name).otherwise(city.name).alias("city_name"),
    "org_id", "org_name", "line_id", "line_name",
    "driver1_emp_id", "driver1_name", "driver2_emp_id", "driver2_name",
    "truck_model_type", "truck_model_type_name", "recent_days",
    "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
    "trans_finish_order_count"
).withColumn("dt", F.lit("20250713"))

print_data_count(final_df, "dws_trans_shift_trans_finish_nd")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_shift_trans_finish_nd/"
)
repair_hive_table("dws_trans_shift_trans_finish_nd")

# ====================== 11. dws_trans_org_deliver_suc_nd ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_org_deliver_suc_nd/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_org_deliver_suc_nd")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_org_deliver_suc_nd (
    org_id        bigint comment '转运站ID',
    org_name      string comment '转运站名称',
    city_id       bigint comment '城市ID',
    city_name     string comment '城市名称',
    province_id   bigint comment '省份ID',
    province_name string comment '省份名称',
    recent_days   tinyint comment '最近天数',
    order_count   bigint comment '派送成功次数（订单数）'
) 
comment '物流域转运站粒度派送成功n日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_org_deliver_suc_nd/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

base_data = spark.table("tms_spark_dws.dws_trans_org_deliver_suc_1d")
# 1. 先通过explode生成recent_days列
exploded = base_data.select(
    "*",
    F.explode(F.array(F.lit(7), F.lit(30))).alias("recent_days")
)

# 2. 再使用recent_days进行过滤
filtered = exploded.filter(
    F.col("dt") >= F.date_add(F.lit("20250713"), F.col("recent_days") * -1 + 1)
)

# 3. 执行分组聚合
final_df = filtered.groupBy(
    "org_id", "org_name", "city_id", "city_name",
    "province_id", "province_name", "recent_days"
).agg(
    F.sum("order_count").alias("order_count")
).withColumn("dt", F.lit("20250713"))

# 后续操作保持不变
print_data_count(final_df, "dws_trans_org_deliver_suc_nd")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_org_deliver_suc_nd/"
)
repair_hive_table("dws_trans_org_deliver_suc_nd")

# ====================== 12. dws_trans_org_sort_nd ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_org_sort_nd/")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_org_sort_nd")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_org_sort_nd (
    org_id        bigint comment '机构ID',
    org_name      string comment '机构名称',
    city_id       bigint comment '城市ID',
    city_name     string comment '城市名称',
    province_id   bigint comment '省份ID',
    province_name string comment '省份名称',
    recent_days   tinyint comment '最近天数',
    sort_count    bigint comment '分拣次数'
) 
comment '物流域机构粒度分拣n日汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_org_sort_nd/'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

base_data = spark.table("tms_spark_dws.dws_trans_org_sort_1d")
exploded = base_data.select(
    "*",
    F.explode(F.array(F.lit(7), F.lit(30))).alias("recent_days")
)

final_df = exploded.groupBy(
    "org_id", "org_name", "city_id", "city_name",
    "province_id", "province_name", "recent_days"
).agg(F.sum("sort_count").alias("sort_count")) \
    .withColumn("dt", F.lit("20250713"))

print_data_count(final_df, "dws_trans_org_sort_nd")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_org_sort_nd/"
)
repair_hive_table("dws_trans_org_sort_nd")

# ====================== 13. dws_trans_dispatch_td ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_dispatch_td")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_dispatch_td")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_dispatch_td (
    order_count  bigint comment '发单数',
    order_amount decimal(16, 2) comment '发单金额'
) 
comment '物流域发单历史至今汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_dispatch_td'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

final_df = spark.table("tms_spark_dws.dws_trans_dispatch_1d") \
    .agg(
    F.sum("order_count").alias("order_count"),
    F.sum("order_amount").alias("order_amount")
).withColumn("dt", F.lit("20250713"))

print_data_count(final_df, "dws_trans_dispatch_td")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_dispatch_td"
)
repair_hive_table("dws_trans_dispatch_td")

# ====================== 14. dws_trans_bound_finish_td ======================
create_hdfs_dir("/warehouse/tms_pyspark/dws/dws_trans_bound_finish_td")
spark.sql("DROP TABLE IF EXISTS tms_spark_dws.dws_trans_bound_finish_td")
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dws.dws_trans_bound_finish_td (
    order_count  bigint comment '发单数',
    order_amount decimal(16, 2) comment '发单金额'
) 
comment '物流域转运完成历史至今汇总表'
PARTITIONED BY (dt string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dws/dws_trans_bound_finish_td'
TBLPROPERTIES ('orc.compress' = 'snappy')
""")

distinct_info = spark.table("tms_spark_dwd.dwd_trans_bound_finish_detail_inc") \
    .groupBy("order_id") \
    .agg(F.max("amount").alias("order_amount"))

final_df = distinct_info.agg(
    F.count("order_id").alias("order_count"),
    F.sum("order_amount").alias("order_amount")
).withColumn("dt", F.lit("20250713"))

print_data_count(final_df, "dws_trans_bound_finish_td")
final_df.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/tms_pyspark/dws/dws_trans_bound_finish_td"
)
repair_hive_table("dws_trans_bound_finish_td")

# 停止SparkSession
spark.stop()