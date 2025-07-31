from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType
from pyspark.sql.functions import *

# 创建SparkSession
spark = SparkSession.builder \
    .appName("WL_Dimension_Tables") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()



# 1. ads_trans_order_stats
spark.sql("DROP TABLE IF EXISTS ads_trans_order_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_trans_order_stats
(
    `dt`                    string COMMENT '统计日期',
    `recent_days`           tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `receive_order_count`   bigint COMMENT '接单总数',
    `receive_order_amount`  decimal(16, 2) COMMENT '接单金额',
    `dispatch_order_count`  bigint COMMENT '发单总数',
    `dispatch_order_amount` decimal(16, 2) COMMENT '发单金额'
) comment '运单相关统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_trans_order_stats'
""")

# PySpark implementation
receive_1d = spark.table("dws_trans_org_receive_1d") \
    .filter(col("dt") == "20250713") \
    .groupBy() \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("receive_order_count"),
    sum("order_amount").alias("receive_order_amount")
)

dispatch_1d = spark.table("dws_trans_dispatch_1d") \
    .filter(col("dt") == "20250713") \
    .select(
    lit(1).alias("recent_days"),
    col("order_count").alias("dispatch_order_count"),
    col("order_amount").alias("dispatch_order_amount")
)

union1 = receive_1d.join(dispatch_1d, "recent_days", "full") \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("receive_order_count"),
    col("receive_order_amount"),
    col("dispatch_order_count"),
    col("dispatch_order_amount")
)

receive_nd = spark.table("dws_trans_org_receive_nd") \
    .filter(col("dt") == "20250713") \
    .groupBy("recent_days") \
    .agg(
    sum("order_count").alias("receive_order_count"),
    sum("order_amount").alias("receive_order_amount")
)

dispatch_nd = spark.table("dws_trans_dispatch_nd") \
    .filter(col("dt") == "20250717") \
    .select(
    "recent_days",
    col("order_count").alias("dispatch_order_count"),
    col("order_amount").alias("dispatch_order_amount")
)

union2 = receive_nd.join(dispatch_nd, "recent_days", "full") \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("receive_order_count"),
    col("receive_order_amount"),
    col("dispatch_order_count"),
    col("dispatch_order_amount")
)

# 直接写入不保留历史记录
union1.union(union2).write.mode("overwrite").saveAsTable("ads_trans_order_stats")

# 2. ads_trans_stats
spark.sql("DROP TABLE IF EXISTS ads_trans_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_trans_stats
(
    `dt`                    string COMMENT '统计日期',
    `recent_days`           tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `trans_finish_count`    bigint COMMENT '完成运输次数',
    `trans_finish_distance` decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`  bigint COMMENT '完成运输时长，单位：秒'
) comment '运输综合统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_trans_stats'
""")

# PySpark implementation
trans_1d = spark.table("dws_trans_org_truck_model_type_trans_finish_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy() \
    .agg(
    lit(1).alias("recent_days"),
    sum("trans_finish_count").alias("trans_finish_count"),
    sum("trans_finish_distance").alias("trans_finish_distance"),
    sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec")
)

trans_nd = spark.table("dws_trans_shift_trans_finish_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("recent_days") \
    .agg(
    sum("trans_finish_count").alias("trans_finish_count"),
    sum("trans_finish_distance").alias("trans_finish_distance"),
    sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec")
)

# 直接写入不保留历史记录
trans_1d.union(trans_nd).write.mode("overwrite").saveAsTable("ads_trans_stats")

# 3. ads_trans_order_stats_td
spark.sql("DROP TABLE IF EXISTS ads_trans_order_stats_td")
spark.sql("""
CREATE EXTERNAL TABLE ads_trans_order_stats_td
(
    `dt`                    string COMMENT '统计日期',
    `bounding_order_count`  bigint COMMENT '运输中运单总数',
    `bounding_order_amount` decimal(16, 2) COMMENT '运输中运单金额'
) comment '历史至今运单统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_trans_order_stats_td'
""")

# PySpark implementation
dispatch_td = spark.table("dws_trans_dispatch_td") \
    .filter(col("dt") == "20250717") \
    .select(
    "dt",
    "order_count",
    "order_amount"
)

bound_finish_td = spark.table("dws_trans_bound_finish_td") \
    .filter(col("dt") == "20250717") \
    .select(
    "dt",
    (col("order_count") * -1).alias("order_count"),
    (col("order_amount") * -1).alias("order_amount")
)

combined_td = dispatch_td.union(bound_finish_td) \
    .groupBy("dt") \
    .agg(
    sum("order_count").alias("bounding_order_count"),
    sum("order_amount").alias("bounding_order_amount")
)

# 直接写入不保留历史记录
combined_td.write.mode("overwrite").saveAsTable("ads_trans_order_stats_td")

# 4. ads_order_stats
spark.sql("DROP TABLE IF EXISTS ads_order_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_order_stats
(
    `dt`           string COMMENT '统计日期',
    `recent_days`  tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `order_count`  bigint COMMENT '下单数',
    `order_amount` decimal(16, 2) COMMENT '下单金额'
) comment '运单综合统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_order_stats'
""")

# PySpark implementation
order_1d = spark.table("dws_trade_org_cargo_type_order_1d") \
    .filter(col("dt") == "20250713") \
    .groupBy() \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("order_count"),
    sum("order_amount").alias("order_amount")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("order_count"),
    col("order_amount")
)

order_nd = spark.table("dws_trade_org_cargo_type_order_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("recent_days") \
    .agg(
    sum("order_count").alias("order_count"),
    sum("order_amount").alias("order_amount")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("order_count"),
    col("order_amount")
)

# 直接写入不保留历史记录
order_1d.union(order_nd).write.mode("overwrite").saveAsTable("ads_order_stats")

# 5. ads_order_cargo_type_stats
spark.sql("DROP TABLE IF EXISTS ads_order_cargo_type_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_order_cargo_type_stats
(
    `dt`              string COMMENT '统计日期',
    `recent_days`     tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `cargo_type`      string COMMENT '货物类型',
    `cargo_type_name` string COMMENT '货物类型名称',
    `order_count`     bigint COMMENT '下单数',
    `order_amount`    decimal(16, 2) COMMENT '下单金额'
) comment '各类型货物运单统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_order_cargo_type_stats'
""")

# PySpark implementation
cargo_1d = spark.table("dws_trade_org_cargo_type_order_1d") \
    .filter(col("dt") == "20250713") \
    .groupBy("cargo_type", "cargo_type_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("order_count"),
    sum("order_amount").alias("order_amount")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("cargo_type"),
    col("cargo_type_name"),
    col("order_count"),
    col("order_amount")
)

cargo_nd = spark.table("dws_trade_org_cargo_type_order_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("cargo_type", "cargo_type_name", "recent_days") \
    .agg(
    sum("order_count").alias("order_count"),
    sum("order_amount").alias("order_amount")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("cargo_type"),
    col("cargo_type_name"),
    col("order_count"),
    col("order_amount")
)

# 直接写入不保留历史记录
cargo_1d.union(cargo_nd).write.mode("overwrite").saveAsTable("ads_order_cargo_type_stats")

# 6. ads_city_stats
# 6. ads_city_stats
spark.sql("DROP TABLE IF EXISTS ads_city_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_city_stats
(
    `dt`                        string COMMENT '统计日期',
    `recent_days`               bigint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `city_id`                   bigint COMMENT '城市ID',
    `city_name`                 string COMMENT '城市名称',
    `order_count`               bigint COMMENT '下单数',
    `order_amount`              decimal COMMENT '下单金额',
    `trans_finish_count`        bigint COMMENT '完成运输次数',
    `trans_finish_distance`     decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`      bigint COMMENT '完成运输时长，单位：秒',
    `avg_trans_finish_distance` decimal(16, 2) COMMENT '平均每次运输里程',
    `avg_trans_finish_dur_sec`  bigint COMMENT '平均每次运输时长，单位：秒'
) comment '城市分析'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_city_stats'
""")

# PySpark implementation for 1 day
city_order_1d = spark.table("dws_trade_org_cargo_type_order_1d") \
    .filter(col("dt") == "20250713") \
    .groupBy("city_id", "city_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("order_count"),
    sum("order_amount").alias("order_amount")
)

dim_organ_full = spark.table("dim_organ_full").filter(col("dt") == "20250713").select("id", "org_level", "region_id").alias("organ")
dim_region_full = spark.table("dim_region_full").filter(col("dt") == "20250717").select("id", "name", "parent_id").alias("region")
dim_region_full2 = spark.table("dim_region_full").filter(col("dt") == "20250717").select(col("id").alias("id2"), col("name").alias("name2")).alias("region2")

city_trans_1d = spark.table("dws_trans_org_truck_model_type_trans_finish_1d") \
    .filter(col("dt") == "20250717") \
    .alias("trans") \
    .join(dim_organ_full, col("trans.org_id") == col("organ.id")) \
    .join(dim_region_full, col("organ.region_id") == col("region.id")) \
    .join(dim_region_full2, col("region.parent_id") == col("region2.id2")) \
    .withColumn("city_id", when(col("organ.org_level") == 1, col("region.id")).otherwise(col("region2.id2"))) \
    .withColumn("city_name", when(col("organ.org_level") == 1, col("region.name")).otherwise(col("region2.name2"))) \
    .groupBy("city_id", "city_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("trans.trans_finish_count").alias("trans_finish_count"),
    sum("trans.trans_finish_distance").alias("trans_finish_distance"),
    sum("trans.trans_finish_dur_sec").alias("trans_finish_dur_sec"),
    (sum("trans.trans_finish_distance") / sum("trans.trans_finish_count")).alias("avg_trans_finish_distance"),
    (sum("trans.trans_finish_dur_sec") / sum("trans.trans_finish_count")).alias("avg_trans_finish_dur_sec")
)

city_stats_1d = city_order_1d.join(
    city_trans_1d,
    ["city_id", "city_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("city_id"),
    col("city_name"),
    col("order_count"),
    col("order_amount"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec"),
    col("avg_trans_finish_distance"),
    col("avg_trans_finish_dur_sec")
)

# PySpark implementation for n days
city_order_nd = spark.table("dws_trade_org_cargo_type_order_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("city_id", "city_name", "recent_days") \
    .agg(
    sum("order_count").alias("order_count"),
    sum("order_amount").alias("order_amount")
)

city_trans_nd = spark.table("dws_trans_shift_trans_finish_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("city_id", "city_name", "recent_days") \
    .agg(
    sum("trans_finish_count").alias("trans_finish_count"),
    sum("trans_finish_distance").alias("trans_finish_distance"),
    sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
    (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
    (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
)

city_stats_nd = city_order_nd.join(
    city_trans_nd,
    ["city_id", "city_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("city_id"),
    col("city_name"),
    col("order_count"),
    col("order_amount"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec"),
    col("avg_trans_finish_distance"),
    col("avg_trans_finish_dur_sec")
)

# 直接写入不保留历史记录
city_stats_1d.union(city_stats_nd).write.mode("overwrite").saveAsTable("ads_city_stats")
# 7. ads_org_stats
spark.sql("DROP TABLE IF EXISTS ads_org_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_org_stats
(
    `dt`                        string COMMENT '统计日期',
    `recent_days`               tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `org_id`                    bigint COMMENT '机构ID',
    `org_name`                  string COMMENT '机构名称',
    `order_count`               bigint COMMENT '下单数',
    `order_amount`              decimal COMMENT '下单金额',
    `trans_finish_count`        bigint COMMENT '完成运输次数',
    `trans_finish_distance`     decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`      bigint COMMENT '完成运输时长，单位：秒',
    `avg_trans_finish_distance` decimal(16, 2) COMMENT '平均每次运输里程',
    `avg_trans_finish_dur_sec`  bigint COMMENT '平均每次运输时长，单位：秒'
) comment '机构分析'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_org_stats'
""")

# PySpark implementation for 1 day
org_order_1d = spark.table("dws_trade_org_cargo_type_order_1d") \
    .filter(col("dt") == "20250713") \
    .groupBy("org_id", "org_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("order_count"),
    sum("order_amount").alias("order_amount")
)

org_trans_1d = spark.table("dws_trans_org_truck_model_type_trans_finish_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy("org_id", "org_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("trans_finish_count").alias("trans_finish_count"),
    sum("trans_finish_distance").alias("trans_finish_distance"),
    sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
    (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
    (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
)

org_stats_1d = org_order_1d.join(
    org_trans_1d,
    ["org_id", "org_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("org_id"),
    col("org_name"),
    col("order_count"),
    col("order_amount"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec"),
    col("avg_trans_finish_distance"),
    col("avg_trans_finish_dur_sec")
)

# PySpark implementation for n days
org_order_nd = spark.table("dws_trade_org_cargo_type_order_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("org_id", "org_name", "recent_days") \
    .agg(
    sum("order_count").alias("order_count"),
    sum("order_amount").alias("order_amount")
)

org_trans_nd = spark.table("dws_trans_shift_trans_finish_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("org_id", "org_name", "recent_days") \
    .agg(
    sum("trans_finish_count").alias("trans_finish_count"),
    sum("trans_finish_distance").alias("trans_finish_distance"),
    sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
    (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
    (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
)

org_stats_nd = org_order_nd.join(
    org_trans_nd,
    ["org_id", "org_name", "recent_days"],
    "inner"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("org_id"),
    col("org_name"),
    col("order_count"),
    col("order_amount"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec"),
    col("avg_trans_finish_distance"),
    col("avg_trans_finish_dur_sec")
)

# 直接写入不保留历史记录
org_stats_1d.union(org_stats_nd).write.mode("overwrite").saveAsTable("ads_org_stats")

# 8. ads_shift_stats
spark.sql("DROP TABLE IF EXISTS ads_shift_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_shift_stats
(
    `dt`                       string COMMENT '统计日期',
    `recent_days`              tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    `shift_id`                 bigint COMMENT '班次ID',
    `trans_finish_count`       bigint COMMENT '完成运输次数',
    `trans_finish_distance`    decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`     bigint COMMENT '完成运输时长，单位：秒',
    `trans_finish_order_count` bigint COMMENT '运输完成运单数'
) comment '班次分析'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_shift_stats'
""")

# PySpark implementation
shift_stats = spark.table("dws_trans_shift_trans_finish_nd") \
    .filter(col("dt") == "20250717") \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("shift_id"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec"),
    col("trans_finish_order_count")
)

# 直接写入不保留历史记录
shift_stats.write.mode("overwrite").saveAsTable("ads_shift_stats")

# 9. ads_line_stats
spark.sql("DROP TABLE IF EXISTS ads_line_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_line_stats
(
    `dt`                       string COMMENT '统计日期',
    `recent_days`              tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    `line_id`                  bigint COMMENT '线路ID',
    `line_name`                string COMMENT '线路名称',
    `trans_finish_count`       bigint COMMENT '完成运输次数',
    `trans_finish_distance`    decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`     bigint COMMENT '完成运输时长，单位：秒',
    `trans_finish_order_count` bigint COMMENT '运输完成运单数'
) comment '线路分析'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_line_stats'
""")

# PySpark implementation
line_stats = spark.table("dws_trans_shift_trans_finish_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("line_id", "line_name", "recent_days") \
    .agg(
    sum("trans_finish_count").alias("trans_finish_count"),
    sum("trans_finish_distance").alias("trans_finish_distance"),
    sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
    sum("trans_finish_order_count").alias("trans_finish_order_count")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("line_id"),
    col("line_name"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec"),
    col("trans_finish_order_count")
)

# 直接写入不保留历史记录
line_stats.write.mode("overwrite").saveAsTable("ads_line_stats")

# 10. ads_driver_stats
spark.sql("DROP TABLE IF EXISTS ads_driver_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_driver_stats
(
    `dt`                        string COMMENT '统计日期',
    `recent_days`               tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    `driver_emp_id`             bigint comment '第一司机员工ID',
    `driver_name`               string comment '第一司机姓名',
    `trans_finish_count`        bigint COMMENT '完成运输次数',
    `trans_finish_distance`     decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`      bigint COMMENT '完成运输时长，单位：秒',
    `avg_trans_finish_distance` decimal(16, 2) COMMENT '平均每次运输里程',
    `avg_trans_finish_dur_sec`  bigint COMMENT '平均每次运输时长，单位：秒'
) comment '司机分析'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_driver_stats'
""")

# PySpark implementation
single_driver = spark.table("dws_trans_shift_trans_finish_nd") \
    .filter((col("dt") == "20250717") & (col("driver2_emp_id").isNull())) \
    .select(
    "recent_days",
    col("driver1_emp_id").alias("driver_id"),
    col("driver1_name").alias("driver_name"),
    "trans_finish_count",
    "trans_finish_distance",
    "trans_finish_dur_sec"
)

dual_driver = spark.table("dws_trans_shift_trans_finish_nd") \
    .filter((col("dt") == "20250717") & (col("driver2_emp_id").isNotNull())) \
    .select(
    "recent_days",
    array(
        array(col("driver1_emp_id"), col("driver1_name")),
        array(col("driver2_emp_id"), col("driver2_name"))
    ).alias("driver_arr"),
    "trans_finish_count",
    (col("trans_finish_distance") / 2).alias("trans_finish_distance"),
    (col("trans_finish_dur_sec") / 2).alias("trans_finish_dur_sec")
) \
    .select(
    "recent_days",
    explode("driver_arr").alias("driver_info"),
    "trans_finish_count",
    "trans_finish_distance",
    "trans_finish_dur_sec"
) \
    .select(
    "recent_days",
    col("driver_info")[0].alias("driver_id"),
    col("driver_info")[1].alias("driver_name"),
    "trans_finish_count",
    "trans_finish_distance",
    "trans_finish_dur_sec"
)

all_drivers = single_driver.union(dual_driver)

driver_stats = all_drivers.groupBy("driver_id", "driver_name", "recent_days") \
    .agg(
    sum("trans_finish_count").alias("trans_finish_count"),
    sum("trans_finish_distance").alias("trans_finish_distance"),
    sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
    (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
    (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("driver_id").alias("driver_emp_id"),
    col("driver_name"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec"),
    col("avg_trans_finish_distance"),
    col("avg_trans_finish_dur_sec")
)

# 直接写入不保留历史记录
driver_stats.write.mode("overwrite").saveAsTable("ads_driver_stats")

# 11. ads_truck_stats
spark.sql("DROP TABLE IF EXISTS ads_truck_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_truck_stats
(
    `dt`                        string COMMENT '统计日期',
    `recent_days`               tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `truck_model_type`          string COMMENT '卡车类别编码',
    `truck_model_type_name`     string COMMENT '卡车类别名称',
    `trans_finish_count`        bigint COMMENT '完成运输次数',
    `trans_finish_distance`     decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`      bigint COMMENT '完成运输时长，单位：秒',
    `avg_trans_finish_distance` decimal(16, 2) COMMENT '平均每次运输里程',
    `avg_trans_finish_dur_sec`  bigint COMMENT '平均每次运输时长，单位：秒'
) comment '卡车分析'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_truck_stats'
""")

# PySpark implementation
truck_stats = spark.table("dws_trans_shift_trans_finish_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("truck_model_type", "truck_model_type_name", "recent_days") \
    .agg(
    sum("trans_finish_count").alias("trans_finish_count"),
    sum("trans_finish_distance").alias("trans_finish_distance"),
    sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
    (sum("trans_finish_distance") / sum("trans_finish_count")).alias("avg_trans_finish_distance"),
    (sum("trans_finish_dur_sec") / sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
) \
    .select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("truck_model_type"),
    col("truck_model_type_name"),
    col("trans_finish_count"),
    col("trans_finish_distance"),
    col("trans_finish_dur_sec"),
    col("avg_trans_finish_distance"),
    col("avg_trans_finish_dur_sec")
)

# 直接写入不保留历史记录
truck_stats.write.mode("overwrite").saveAsTable("ads_truck_stats")

# 12. ads_express_stats
spark.sql("DROP TABLE IF EXISTS ads_express_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_express_stats
(
    `dt`                string COMMENT '统计日期',
    `recent_days`       tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `deliver_suc_count` bigint COMMENT '派送成功次数（订单数）',
    `sort_count`        bigint COMMENT '分拣次数'
) comment '快递综合统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_express_stats'
""")

# PySpark implementation for 1 day
deliver_1d = spark.table("dws_trans_org_deliver_suc_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy() \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("deliver_suc_count")
)

sort_1d = spark.table("dws_trans_org_sort_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy() \
    .agg(
    lit(1).alias("recent_days"),
    sum("sort_count").alias("sort_count")
)

express_stats_1d = deliver_1d.join(
    sort_1d,
    "recent_days",
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("deliver_suc_count"),
    col("sort_count")
)

# PySpark implementation for n days
deliver_nd = spark.table("dws_trans_org_deliver_suc_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("recent_days") \
    .agg(
    sum("order_count").alias("deliver_suc_count")
)

sort_nd = spark.table("dws_trans_org_sort_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("recent_days") \
    .agg(
    sum("sort_count").alias("sort_count")
)

express_stats_nd = deliver_nd.join(
    sort_nd,
    "recent_days",
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("deliver_suc_count"),
    col("sort_count")
)

# 直接写入不保留历史记录
express_stats_1d.union(express_stats_nd).write.mode("overwrite").saveAsTable("ads_express_stats")

# 13. ads_express_province_stats
spark.sql("DROP TABLE IF EXISTS ads_express_province_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_express_province_stats
(
    `dt`                   string COMMENT '统计日期',
    `recent_days`          tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`          bigint COMMENT '省份ID',
    `province_name`        string COMMENT '省份名称',
    `receive_order_count`  bigint COMMENT '揽收次数',
    `receive_order_amount` decimal(16, 2) COMMENT '揽收金额',
    `deliver_suc_count`    bigint COMMENT '派送成功次数',
    `sort_count`           bigint COMMENT '分拣次数'
) comment '各省份快递统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_express_province_stats'
""")

# PySpark implementation for 1 day
province_deliver_1d = spark.table("dws_trans_org_deliver_suc_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy("province_id", "province_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("deliver_suc_count")
)

province_sort_1d = spark.table("dws_trans_org_sort_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy("province_id", "province_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("sort_count").alias("sort_count")
)

province_receive_1d = spark.table("dws_trans_org_receive_1d") \
    .filter(col("dt") == "20250713") \
    .groupBy("province_id", "province_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("receive_order_count"),
    sum("order_amount").alias("receive_order_amount")
)

# Join all three datasets
province_stats_1d = province_deliver_1d.join(
    province_sort_1d,
    ["province_id", "province_name", "recent_days"],
    "full"
).join(
    province_receive_1d,
    ["province_id", "province_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("province_id"),
    col("province_name"),
    col("receive_order_count"),
    col("receive_order_amount"),
    col("deliver_suc_count"),
    col("sort_count")
)

# PySpark implementation for n days
province_deliver_nd = spark.table("dws_trans_org_deliver_suc_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("province_id", "province_name", "recent_days") \
    .agg(
    sum("order_count").alias("deliver_suc_count")
)

province_sort_nd = spark.table("dws_trans_org_sort_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("province_id", "province_name", "recent_days") \
    .agg(
    sum("sort_count").alias("sort_count")
)

province_receive_nd = spark.table("dws_trans_org_receive_nd") \
    .filter(col("dt") == "20250713") \
    .groupBy("province_id", "province_name", "recent_days") \
    .agg(
    sum("order_count").alias("receive_order_count"),
    sum("order_amount").alias("receive_order_amount")
)

# Join all three datasets
province_stats_nd = province_deliver_nd.join(
    province_sort_nd,
    ["province_id", "province_name", "recent_days"],
    "full"
).join(
    province_receive_nd,
    ["province_id", "province_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("province_id"),
    col("province_name"),
    col("receive_order_count"),
    col("receive_order_amount"),
    col("deliver_suc_count"),
    col("sort_count")
)

# 直接写入不保留历史记录
province_stats_1d.union(province_stats_nd).write.mode("overwrite").saveAsTable("ads_express_province_stats")

# 14. ads_express_city_stats
spark.sql("DROP TABLE IF EXISTS ads_express_city_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_express_city_stats
(
    `dt`                   string COMMENT '统计日期',
    `recent_days`          tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `city_id`              bigint COMMENT '城市ID',
    `city_name`            string COMMENT '城市名称',
    `receive_order_count`  bigint COMMENT '揽收次数',
    `receive_order_amount` decimal(16, 2) COMMENT '揽收金额',
    `deliver_suc_count`    bigint COMMENT '派送成功次数',
    `sort_count`           bigint COMMENT '分拣次数'
) comment '各城市快递统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_express_city_stats'
""")

# PySpark implementation for 1 day
city_deliver_1d = spark.table("dws_trans_org_deliver_suc_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy("city_id", "city_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("deliver_suc_count")
)

city_sort_1d = spark.table("dws_trans_org_sort_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy("city_id", "city_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("sort_count").alias("sort_count")
)

city_receive_1d = spark.table("dws_trans_org_receive_1d") \
    .filter(col("dt") == "20250713") \
    .groupBy("city_id", "city_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("receive_order_count"),
    sum("order_amount").alias("receive_order_amount")
)

# Join all three datasets
city_stats_1d = city_deliver_1d.join(
    city_sort_1d,
    ["city_id", "city_name", "recent_days"],
    "full"
).join(
    city_receive_1d,
    ["city_id", "city_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("city_id"),
    col("city_name"),
    col("receive_order_count"),
    col("receive_order_amount"),
    col("deliver_suc_count"),
    col("sort_count")
)

# PySpark implementation for n days
city_deliver_nd = spark.table("dws_trans_org_deliver_suc_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("city_id", "city_name", "recent_days") \
    .agg(
    sum("order_count").alias("deliver_suc_count")
)

city_sort_nd = spark.table("dws_trans_org_sort_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("city_id", "city_name", "recent_days") \
    .agg(
    sum("sort_count").alias("sort_count")
)

city_receive_nd = spark.table("dws_trans_org_receive_nd") \
    .filter(col("dt") == "20250713") \
    .groupBy("city_id", "city_name", "recent_days") \
    .agg(
    sum("order_count").alias("receive_order_count"),
    sum("order_amount").alias("receive_order_amount")
)

# Join all three datasets
city_stats_nd = city_deliver_nd.join(
    city_sort_nd,
    ["city_id", "city_name", "recent_days"],
    "full"
).join(
    city_receive_nd,
    ["city_id", "city_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("city_id"),
    col("city_name"),
    col("receive_order_count"),
    col("receive_order_amount"),
    col("deliver_suc_count"),
    col("sort_count")
)

# 直接写入不保留历史记录
city_stats_1d.union(city_stats_nd).write.mode("overwrite").saveAsTable("ads_express_city_stats")

# 15. ads_express_org_stats
spark.sql("DROP TABLE IF EXISTS ads_express_org_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_express_org_stats
(
    `dt`                   string COMMENT '统计日期',
    `recent_days`          tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `org_id`               bigint COMMENT '机构ID',
    `org_name`             string COMMENT '机构名称',
    `receive_order_count`  bigint COMMENT '揽收次数',
    `receive_order_amount` decimal(16, 2) COMMENT '揽收金额',
    `deliver_suc_count`    bigint COMMENT '派送成功次数',
    `sort_count`           bigint COMMENT '分拣次数'
) comment '各机构快递统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms_spark/ads/ads_express_org_stats'
""")

# PySpark implementation for 1 day
org_deliver_1d = spark.table("dws_trans_org_deliver_suc_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy("org_id", "org_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("deliver_suc_count")
)

org_sort_1d = spark.table("dws_trans_org_sort_1d") \
    .filter(col("dt") == "20250717") \
    .groupBy("org_id", "org_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("sort_count").alias("sort_count")
)

org_receive_1d = spark.table("dws_trans_org_receive_1d") \
    .filter(col("dt") == "20250713") \
    .groupBy("org_id", "org_name") \
    .agg(
    lit(1).alias("recent_days"),
    sum("order_count").alias("receive_order_count"),
    sum("order_amount").alias("receive_order_amount")
)

# Join all three datasets
org_stats_1d = org_deliver_1d.join(
    org_sort_1d,
    ["org_id", "org_name", "recent_days"],
    "full"
).join(
    org_receive_1d,
    ["org_id", "org_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("org_id"),
    col("org_name"),
    col("receive_order_count"),
    col("receive_order_amount"),
    col("deliver_suc_count"),
    col("sort_count")
)

# PySpark implementation for n days
org_deliver_nd = spark.table("dws_trans_org_deliver_suc_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("org_id", "org_name", "recent_days") \
    .agg(
    sum("order_count").alias("deliver_suc_count")
)

org_sort_nd = spark.table("dws_trans_org_sort_nd") \
    .filter(col("dt") == "20250717") \
    .groupBy("org_id", "org_name", "recent_days") \
    .agg(
    sum("sort_count").alias("sort_count")
)

org_receive_nd = spark.table("dws_trans_org_receive_nd") \
    .filter(col("dt") == "20250713") \
    .groupBy("org_id", "org_name", "recent_days") \
    .agg(
    sum("order_count").alias("receive_order_count"),
    sum("order_amount").alias("receive_order_amount")
)

# Join all three datasets
org_stats_nd = org_deliver_nd.join(
    org_sort_nd,
    ["org_id", "org_name", "recent_days"],
    "full"
).join(
    org_receive_nd,
    ["org_id", "org_name", "recent_days"],
    "full"
).select(
    lit("20250717").alias("dt"),
    col("recent_days"),
    col("org_id"),
    col("org_name"),
    col("receive_order_count"),
    col("receive_order_amount"),
    col("deliver_suc_count"),
    col("sort_count")
)

# 直接写入不保留历史记录
org_stats_1d.union(org_stats_nd).write.mode("overwrite").saveAsTable("ads_express_org_stats")