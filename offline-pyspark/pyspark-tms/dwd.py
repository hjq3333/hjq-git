from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("DWD_Trade_Order_Detail_Inc") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()

# 导入Java类用于HDFS操作
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

# 工具函数定义
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
    spark.sql(f"MSCK REPAIR TABLE tms_spark_dwd.{table_name}")
    print(f"修复分区完成：tms_spark_dwd.{table_name}")

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count




# ====================== 交易域订单明细事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trade_order_detail_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trade_order_detail_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trade_order_detail_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `order_time`           string COMMENT '下单时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
) comment '交易域订单明细事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms/dwd/dwd_trade_order_detail_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight"),
    # 处理下单时间格式
    F.concat(
        F.substring(F.col("create_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("create_time"), 12, 8)
    ).alias("order_time")
)

# 4.2 读取订单信息
info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏（保留首字符）
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("cargo_num"),
    F.col("amount"),
    # 处理预计到达时间
    F.date_format(
        F.from_utc_timestamp(F.col("estimate_arrive_time").cast(T.LongType()).cast(T.TimestampType()), "UTC"),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance")
)

# 4.3 读取字典表（货物类型）
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("name")
)

# 4.4 读取字典表（订单状态）
status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("name")
)

# 4.5 读取字典表（取件类型）
collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("name")
)

# 5. 多表关联（修改dt生成逻辑）
joined_df = cargo_df.alias("c") \
    .join(info_df.alias("i"), F.col("c.order_id") == F.col("i.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("i.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("i.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.coalesce(F.col("ct.name"), F.lit("")).alias("cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("c.order_time"),
    F.col("i.order_no"),
    F.col("i.status"),
    F.coalesce(F.col("s.name"), F.lit("")).alias("status_name"),
    F.col("i.collect_type"),
    F.coalesce(F.col("colt.name"), F.lit("")).alias("collect_type_name"),
    F.col("i.user_id"),
    F.col("i.receiver_complex_id"),
    F.col("i.receiver_province_id"),
    F.col("i.receiver_city_id"),
    F.col("i.receiver_district_id"),
    F.col("i.receiver_name"),
    F.col("i.sender_complex_id"),
    F.col("i.sender_province_id"),
    F.col("i.sender_city_id"),
    F.col("i.sender_district_id"),
    F.col("i.sender_name"),
    F.col("i.cargo_num"),
    F.col("i.amount"),
    F.col("i.estimate_arrive_time"),
    F.col("i.distance"),
    # 使用当前日期作为dt分区字段（格式：yyyy-MM-dd）
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trade_order_detail_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms/dwd/dwd_trade_order_detail_inc")

# 8. 修复分区
repair_hive_table("dwd_trade_order_detail_inc")




# ====================== 交易域取消运单事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trade_order_cancel_detail_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trade_order_cancel_detail_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trade_order_cancel_detail_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `cancel_time`          string COMMENT '取消时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
) comment '交易域取消运单事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trade_order_cancel_detail_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息（与订单明细表一致）
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight")
)

# 4.2 读取取消订单信息（新增：过滤status=60020）
cancel_info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") &
    (F.col("is_deleted") == "0") &
    (F.col("status") == "60020")  # 只保留取消状态的订单
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("cargo_num"),
    F.col("amount"),
    # 处理预计到达时间（修复类型不匹配问题）
    F.date_format(
        # 关键修复：先将 BIGINT 时间戳转为 TIMESTAMP，再调整时区
        F.from_utc_timestamp(
            F.to_timestamp(F.col("estimate_arrive_time").cast(T.LongType()) / 1000),  # 毫秒→秒（若原始是秒级可省略/1000）
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance"),
    # 处理取消时间（从update_time字段提取）
    F.concat(
        F.substring(F.col("update_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("update_time"), 12, 8)
    ).alias("cancel_time")
)

# 4.3-4.5 读取字典表（与订单明细表一致）
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("cargo_type_name"))

status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("status_name"))

collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("collect_type_name"))

# 5. 多表关联
joined_df = cargo_df.alias("c") \
    .join(cancel_info_df.alias("ci"), F.col("c.order_id") == F.col("ci.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("ci.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("ci.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.col("ct.cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("ci.cancel_time"),
    F.col("ci.order_no"),
    F.col("ci.status"),
    F.col("s.status_name"),
    F.col("ci.collect_type"),
    F.col("colt.collect_type_name"),
    F.col("ci.user_id"),
    F.col("ci.receiver_complex_id"),
    F.col("ci.receiver_province_id"),
    F.col("ci.receiver_city_id"),
    F.col("ci.receiver_district_id"),
    F.col("ci.receiver_name"),
    F.col("ci.sender_complex_id"),
    F.col("ci.sender_province_id"),
    F.col("ci.sender_city_id"),
    F.col("ci.sender_district_id"),
    F.col("ci.sender_name"),
    F.col("ci.cargo_num"),
    F.col("ci.amount"),
    F.col("ci.estimate_arrive_time"),
    F.col("ci.distance"),
    # 使用当前日期作为dt分区字段
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trade_order_cancel_detail_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trade_order_cancel_detail_inc")

# 8. 修复分区
repair_hive_table("dwd_trade_order_cancel_detail_inc")


# ====================== 物流域揽收事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trans_receive_detail_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trans_receive_detail_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trans_receive_detail_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `receive_time`         string COMMENT '揽收时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `payment_type`         string COMMENT '支付方式',
    `payment_type_name`    string COMMENT '支付方式名称',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
) comment '物流域揽收事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trans_receive_detail_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight")
)

# 4.2 读取揽收订单信息（过滤掉已取消、待支付和已关闭状态）
receive_info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") &
    (F.col("is_deleted") == "0") &
    ~F.col("status").isin("60010", "60020", "60999")  # 排除待支付、已取消、已关闭
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("payment_type"),
    F.col("cargo_num"),
    F.col("amount"),
    # 处理预计到达时间
    # 处理预计到达时间（修复类型不匹配问题）
    F.date_format(
        # 关键修复：先将 BIGINT 时间戳转为 TIMESTAMP，再调整时区
        F.from_utc_timestamp(
            F.to_timestamp(F.col("estimate_arrive_time").cast(T.LongType()) / 1000),  # 毫秒→秒（若原始是秒级可省略/1000）
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance"),
    # 处理揽收时间（从update_time字段提取）
    F.concat(
        F.substring(F.col("update_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("update_time"), 12, 8)
    ).alias("receive_time")
)

# 4.3-4.6 读取字典表
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("cargo_type_name"))

status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("status_name"))

collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("collect_type_name"))

payment_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("payment_type_name"))

# 5. 多表关联
joined_df = cargo_df.alias("c") \
    .join(receive_info_df.alias("ri"), F.col("c.order_id") == F.col("ri.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("ri.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("ri.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .join(payment_type_dic.alias("pt"), F.col("ri.payment_type") == F.col("pt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.col("ct.cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("ri.receive_time"),
    F.col("ri.order_no"),
    F.col("ri.status"),
    F.col("s.status_name"),
    F.col("ri.collect_type"),
    F.col("colt.collect_type_name"),
    F.col("ri.user_id"),
    F.col("ri.receiver_complex_id"),
    F.col("ri.receiver_province_id"),
    F.col("ri.receiver_city_id"),
    F.col("ri.receiver_district_id"),
    F.col("ri.receiver_name"),
    F.col("ri.sender_complex_id"),
    F.col("ri.sender_province_id"),
    F.col("ri.sender_city_id"),
    F.col("ri.sender_district_id"),
    F.col("ri.sender_name"),
    F.col("ri.payment_type"),
    F.col("pt.payment_type_name"),
    F.col("ri.cargo_num"),
    F.col("ri.amount"),
    F.col("ri.estimate_arrive_time"),
    F.col("ri.distance"),
    # 使用当前日期作为dt分区字段
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trans_receive_detail_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trans_receive_detail_inc")

# 8. 修复分区
repair_hive_table("dwd_trans_receive_detail_inc")


# ====================== 物流域发单事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trans_dispatch_detail_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trans_dispatch_detail_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trans_dispatch_detail_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `dispatch_time`        string COMMENT '发单时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `payment_type`         string COMMENT '支付方式',
    `payment_type_name`    string COMMENT '支付方式名称',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
) comment '物流域发单事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trans_dispatch_detail_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight")
)

# 4.2 读取发单订单信息（过滤掉待支付、已取消、待揽收、已揽收、已关闭状态）
dispatch_info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") &
    (F.col("is_deleted") == "0") &
    ~F.col("status").isin("60010", "60020", "60030", "60040", "60999")  # 排除特定状态
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("payment_type"),
    F.col("cargo_num"),
    F.col("amount"),
    # 处理预计到达时间
    # 处理预计到达时间（修复类型不匹配问题）
    F.date_format(
        # 关键修复：先将 BIGINT 时间戳转为 TIMESTAMP，再调整时区
        F.from_utc_timestamp(
            F.to_timestamp(F.col("estimate_arrive_time").cast(T.LongType()) / 1000),  # 毫秒→秒（若原始是秒级可省略/1000）
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance"),
    # 处理发单时间（从update_time字段提取）
    F.concat(
        F.substring(F.col("update_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("update_time"), 12, 8)
    ).alias("dispatch_time")
)

# 4.3-4.6 读取字典表
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("cargo_type_name"))

status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("status_name"))

collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("collect_type_name"))

payment_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("payment_type_name"))

# 5. 多表关联
joined_df = cargo_df.alias("c") \
    .join(dispatch_info_df.alias("di"), F.col("c.order_id") == F.col("di.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("di.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("di.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .join(payment_type_dic.alias("pt"), F.col("di.payment_type") == F.col("pt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.col("ct.cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("di.dispatch_time"),
    F.col("di.order_no"),
    F.col("di.status"),
    F.col("s.status_name"),
    F.col("di.collect_type"),
    F.col("colt.collect_type_name"),
    F.col("di.user_id"),
    F.col("di.receiver_complex_id"),
    F.col("di.receiver_province_id"),
    F.col("di.receiver_city_id"),
    F.col("di.receiver_district_id"),
    F.col("di.receiver_name"),
    F.col("di.sender_complex_id"),
    F.col("di.sender_province_id"),
    F.col("di.sender_city_id"),
    F.col("di.sender_district_id"),
    F.col("di.sender_name"),
    F.col("di.payment_type"),
    F.col("pt.payment_type_name"),
    F.col("di.cargo_num"),
    F.col("di.amount"),
    F.col("di.estimate_arrive_time"),
    F.col("di.distance"),
    # 使用当前日期作为dt分区字段
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trans_dispatch_detail_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trans_dispatch_detail_inc")

# 8. 修复分区
repair_hive_table("dwd_trans_dispatch_detail_inc")



# ====================== 物流域转运完成事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trans_bound_finish_detail_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trans_bound_finish_detail_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trans_bound_finish_detail_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `bound_finish_time`    string COMMENT '转运完成时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `payment_type`         string COMMENT '支付方式',
    `payment_type_name`    string COMMENT '支付方式名称',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
) comment '物流域转运完成事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trans_bound_finish_detail_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight")
)

# 4.2 读取转运完成订单信息（过滤掉已取消、待揽收、已揽收、运输中状态）
bound_finish_info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") &
    (F.col("is_deleted") == "0") &
    ~F.col("status").isin("60020", "60030", "60040", "60050")  # 排除特定状态
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("payment_type"),
    F.col("cargo_num"),
    F.col("amount"),
    # 处理预计到达时间
    # 处理预计到达时间（修复类型不匹配问题）
    F.date_format(
        # 关键修复：先将 BIGINT 时间戳转为 TIMESTAMP，再调整时区
        F.from_utc_timestamp(
            F.to_timestamp(F.col("estimate_arrive_time").cast(T.LongType()) / 1000),  # 毫秒→秒（若原始是秒级可省略/1000）
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance"),
    # 处理转运完成时间（从update_time字段提取）
    F.concat(
        F.substring(F.col("update_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("update_time"), 12, 8)
    ).alias("bound_finish_time")
)

# 4.3-4.6 读取字典表
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("cargo_type_name"))

status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("status_name"))

collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("collect_type_name"))

payment_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("payment_type_name"))

# 5. 多表关联
joined_df = cargo_df.alias("c") \
    .join(bound_finish_info_df.alias("bfi"), F.col("c.order_id") == F.col("bfi.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("bfi.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("bfi.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .join(payment_type_dic.alias("pt"), F.col("bfi.payment_type") == F.col("pt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.col("ct.cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("bfi.bound_finish_time"),
    F.col("bfi.order_no"),
    F.col("bfi.status"),
    F.col("s.status_name"),
    F.col("bfi.collect_type"),
    F.col("colt.collect_type_name"),
    F.col("bfi.user_id"),
    F.col("bfi.receiver_complex_id"),
    F.col("bfi.receiver_province_id"),
    F.col("bfi.receiver_city_id"),
    F.col("bfi.receiver_district_id"),
    F.col("bfi.receiver_name"),
    F.col("bfi.sender_complex_id"),
    F.col("bfi.sender_province_id"),
    F.col("bfi.sender_city_id"),
    F.col("bfi.sender_district_id"),
    F.col("bfi.sender_name"),
    F.col("bfi.payment_type"),
    F.col("pt.payment_type_name"),
    F.col("bfi.cargo_num"),
    F.col("bfi.amount"),
    F.col("bfi.estimate_arrive_time"),
    F.col("bfi.distance"),
    # 使用当前日期作为dt分区字段
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trans_bound_finish_detail_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trans_bound_finish_detail_inc")

# 8. 修复分区
repair_hive_table("dwd_trans_bound_finish_detail_inc")


# ====================== 物流域派送成功事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trans_deliver_suc_detail_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trans_deliver_suc_detail_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trans_deliver_suc_detail_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `deliver_suc_time`     string COMMENT '派送成功时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `payment_type`         string COMMENT '支付方式',
    `payment_type_name`    string COMMENT '支付方式名称',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
) comment '物流域派送成功事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trans_deliver_suc_detail_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight")
)

# 4.2 读取派送成功订单信息（过滤掉已取消、待揽收、已揽收状态）
deliver_suc_info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") &
    (F.col("is_deleted") == "0") &
    ~F.col("status").isin("60020", "60030", "60040")  # 排除特定状态
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("payment_type"),
    F.col("cargo_num"),
    F.col("amount"),
    # 处理预计到达时间
    # 处理预计到达时间（修复类型不匹配问题）
    F.date_format(
        # 关键修复：先将 BIGINT 时间戳转为 TIMESTAMP，再调整时区
        F.from_utc_timestamp(
            F.to_timestamp(F.col("estimate_arrive_time").cast(T.LongType()) / 1000),  # 毫秒→秒（若原始是秒级可省略/1000）
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance"),
    # 处理派送成功时间（从update_time字段提取）
    F.concat(
        F.substring(F.col("update_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("update_time"), 12, 8)
    ).alias("deliver_suc_time")
)

# 4.3-4.6 读取字典表
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("cargo_type_name"))

status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("status_name"))

collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("collect_type_name"))

payment_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("payment_type_name"))

# 5. 多表关联
joined_df = cargo_df.alias("c") \
    .join(deliver_suc_info_df.alias("dsi"), F.col("c.order_id") == F.col("dsi.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("dsi.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("dsi.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .join(payment_type_dic.alias("pt"), F.col("dsi.payment_type") == F.col("pt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.col("ct.cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("dsi.deliver_suc_time"),
    F.col("dsi.order_no"),
    F.col("dsi.status"),
    F.col("s.status_name"),
    F.col("dsi.collect_type"),
    F.col("colt.collect_type_name"),
    F.col("dsi.user_id"),
    F.col("dsi.receiver_complex_id"),
    F.col("dsi.receiver_province_id"),
    F.col("dsi.receiver_city_id"),
    F.col("dsi.receiver_district_id"),
    F.col("dsi.receiver_name"),
    F.col("dsi.sender_complex_id"),
    F.col("dsi.sender_province_id"),
    F.col("dsi.sender_city_id"),
    F.col("dsi.sender_district_id"),
    F.col("dsi.sender_name"),
    F.col("dsi.payment_type"),
    F.col("pt.payment_type_name"),
    F.col("dsi.cargo_num"),
    F.col("dsi.amount"),
    F.col("dsi.estimate_arrive_time"),
    F.col("dsi.distance"),
    # 使用派送成功时间的日期作为dt分区字段
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trans_deliver_suc_detail_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trans_deliver_suc_detail_inc")

# 8. 修复分区
repair_hive_table("dwd_trans_deliver_suc_detail_inc")



# ====================== 物流域签收事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trans_sign_detail_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trans_sign_detail_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trans_sign_detail_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `sign_time`            string COMMENT '签收时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `payment_type`         string COMMENT '支付方式',
    `payment_type_name`    string COMMENT '支付方式名称',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
) comment '物流域签收事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trans_sign_detail_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight")
)

# 4.2 读取签收订单信息（过滤掉已取消、待揽收、已揽收、运输中状态）
sign_info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") &
    (F.col("is_deleted") == "0") &
    ~F.col("status").isin("60020", "60030", "60040", "60050")  # 排除特定状态
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("payment_type"),
    F.col("cargo_num"),
    F.col("amount"),
    # 处理预计到达时间
    # 处理预计到达时间（修复类型不匹配问题）
    F.date_format(
        # 关键修复：先将 BIGINT 时间戳转为 TIMESTAMP，再调整时区
        F.from_utc_timestamp(
            F.to_timestamp(F.col("estimate_arrive_time").cast(T.LongType()) / 1000),  # 毫秒→秒（若原始是秒级可省略/1000）
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance"),
    # 处理签收时间（从update_time字段提取）
    F.concat(
        F.substring(F.col("update_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("update_time"), 12, 8)
    ).alias("sign_time")
)

# 4.3-4.6 读取字典表
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("cargo_type_name"))

status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("status_name"))

collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("collect_type_name"))

payment_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("payment_type_name"))

# 5. 多表关联
joined_df = cargo_df.alias("c") \
    .join(sign_info_df.alias("si"), F.col("c.order_id") == F.col("si.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("si.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("si.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .join(payment_type_dic.alias("pt"), F.col("si.payment_type") == F.col("pt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.col("ct.cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("si.sign_time"),
    F.col("si.order_no"),
    F.col("si.status"),
    F.col("s.status_name"),
    F.col("si.collect_type"),
    F.col("colt.collect_type_name"),
    F.col("si.user_id"),
    F.col("si.receiver_complex_id"),
    F.col("si.receiver_province_id"),
    F.col("si.receiver_city_id"),
    F.col("si.receiver_district_id"),
    F.col("si.receiver_name"),
    F.col("si.sender_complex_id"),
    F.col("si.sender_province_id"),
    F.col("si.sender_city_id"),
    F.col("si.sender_district_id"),
    F.col("si.sender_name"),
    F.col("si.payment_type"),
    F.col("pt.payment_type_name"),
    F.col("si.cargo_num"),
    F.col("si.amount"),
    F.col("si.estimate_arrive_time"),
    F.col("si.distance"),
    # 使用签收时间的日期作为dt分区字段
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trans_sign_detail_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trans_sign_detail_inc")

# 8. 修复分区
repair_hive_table("dwd_trans_sign_detail_inc")


# ====================== 交易域运单累积快照事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trade_order_process_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trade_order_process_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trade_order_process_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `order_time`           string COMMENT '下单时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `payment_type`         string COMMENT '支付方式',
    `payment_type_name`    string COMMENT '支付方式名称',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里',
    `start_date`           string COMMENT '开始日期',
    `end_date`             string COMMENT '结束日期'
) comment '交易域运单累积快照事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trade_order_process_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息（包含下单时间处理）
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight"),
    # 处理下单时间（从create_time字段提取）
    F.concat(
        F.substring(F.col("create_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("create_time"), 12, 8)
    ).alias("order_time")
)

# 4.2 读取运单信息（处理结束日期逻辑）
order_process_info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("payment_type"),
    F.col("cargo_num"),
    F.col("amount"),
    # 处理预计到达时间
    # 处理预计到达时间（修复类型不匹配问题）
    F.date_format(
        # 关键修复：先将 BIGINT 时间戳转为 TIMESTAMP，再调整时区
        F.from_utc_timestamp(
            F.to_timestamp(F.col("estimate_arrive_time").cast(T.LongType()) / 1000),  # 毫秒→秒（若原始是秒级可省略/1000）
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance"),
    # 处理结束日期（状态为60020或60030时取update_time日期，否则为9999-12-31）
    F.when(
        F.col("status").isin("60020", "60030"),
        F.substring(F.col("update_time"), 1, 10)
    ).otherwise(F.lit("9999-12-31")).alias("end_date")
)

# 4.3-4.6 读取字典表
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("cargo_type_name"))

status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("status_name"))

collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("collect_type_name"))

payment_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("payment_type_name"))

# 5. 多表关联
joined_df = cargo_df.alias("c") \
    .join(order_process_info_df.alias("opi"), F.col("c.order_id") == F.col("opi.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("opi.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("opi.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .join(payment_type_dic.alias("pt"), F.col("opi.payment_type") == F.col("pt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.col("ct.cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("c.order_time"),
    F.col("opi.order_no"),
    F.col("opi.status"),
    F.col("s.status_name"),
    F.col("opi.collect_type"),
    F.col("colt.collect_type_name"),
    F.col("opi.user_id"),
    F.col("opi.receiver_complex_id"),
    F.col("opi.receiver_province_id"),
    F.col("opi.receiver_city_id"),
    F.col("opi.receiver_district_id"),
    F.col("opi.receiver_name"),
    F.col("opi.sender_complex_id"),
    F.col("opi.sender_province_id"),
    F.col("opi.sender_city_id"),
    F.col("opi.sender_district_id"),
    F.col("opi.sender_name"),
    F.col("opi.payment_type"),
    F.col("pt.payment_type_name"),
    F.col("opi.cargo_num"),
    F.col("opi.amount"),
    F.col("opi.estimate_arrive_time"),
    F.col("opi.distance"),
    # 开始日期：下单时间的日期部分
    F.date_format(F.col("c.order_time"), "yyyy-MM-dd").alias("start_date"),
    F.col("opi.end_date"),
    # 分区字段使用结束日期
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trade_order_process_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trade_order_process_inc")

# 8. 修复分区
repair_hive_table("dwd_trade_order_process_inc")


# ====================== 物流域运输事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trans_trans_finish_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trans_trans_finish_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trans_trans_finish_inc (
    `id`                bigint comment '运输任务ID',
    `shift_id`          bigint COMMENT '车次ID',
    `line_id`           bigint COMMENT '路线ID',
    `start_org_id`      bigint COMMENT '起始机构ID',
    `start_org_name`    string COMMENT '起始机构名称',
    `end_org_id`        bigint COMMENT '目的机构ID',
    `end_org_name`      string COMMENT '目的机构名称',
    `order_num`         bigint COMMENT '运单个数',
    `driver1_emp_id`    bigint COMMENT '司机1ID',
    `driver1_name`      string COMMENT '司机1名称',
    `driver2_emp_id`    bigint COMMENT '司机2ID',
    `driver2_name`      string COMMENT '司机2名称',
    `truck_id`          bigint COMMENT '卡车ID',
    `truck_no`          string COMMENT '卡车号牌',
    `actual_start_time` string COMMENT '实际启动时间',
    `actual_end_time`   string COMMENT '实际到达时间',
    `actual_distance`   decimal(16, 2) COMMENT '实际行驶距离',
    `finish_dur_sec`    bigint COMMENT '运输完成历经时长：秒'
) comment '物流域运输事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trans_trans_finish_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取运输任务信息（ods_transport_task）
transport_task_df = spark.table("wl.ods_transport_task").filter(
    (F.col("dt") == "20250721") &
    (F.col("actual_end_time").isNotNull()) &
    (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("shift_id"),
    F.col("line_id"),
    F.col("start_org_id"),
    F.col("start_org_name"),
    F.col("end_org_id"),
    F.col("end_org_name"),
    F.col("order_num"),
    F.col("driver1_emp_id"),
    # 司机1姓名脱敏：保留首字符+*
    F.concat(F.substring(F.col("driver1_name"), 1, 1), F.lit("*")).alias("driver1_name"),
    F.col("driver2_emp_id"),
    # 司机2姓名脱敏：保留首字符+*
    F.concat(F.substring(F.col("driver2_name"), 1, 1), F.lit("*")).alias("driver2_name"),
    F.col("truck_id"),
    # 卡车号牌MD5加密
    F.md5(F.col("truck_no")).alias("truck_no"),
    # 处理实际启动时间（修复类型问题）
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("actual_start_time").cast(T.LongType()) / 1000),  # 先转为 TIMESTAMP
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("actual_start_time"),

    # 处理实际到达时间（同理修复）
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("actual_end_time").cast(T.LongType()) / 1000),  # 先转为 TIMESTAMP
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("actual_end_time"),
    F.col("actual_distance"),
    # 计算运输时长（秒）：(结束时间戳 - 开始时间戳) / 1000
    ((F.col("actual_end_time").cast(T.LongType()) - F.col("actual_start_time").cast(T.LongType())) / 1000).cast(T.LongType()).alias("finish_dur_sec")
)

# 4.2 读取车次维度表（dim_shift_full）
dim_shift_df = spark.table("wl.dim_shift_full").filter(
    F.col("dt") == "20250721"
).select(
    F.col("id").alias("shift_id")  # 重命名用于关联
)

# 5. 表关联（运输任务与车次维度表左关联）
joined_df = transport_task_df.alias("tt") \
    .join(
    dim_shift_df.alias("ds"),
    F.col("tt.shift_id") == F.col("ds.shift_id"),
    "left"
).select(
    F.col("tt.id"),
    F.col("tt.shift_id"),
    F.col("tt.line_id"),
    F.col("tt.start_org_id"),
    F.col("tt.start_org_name"),
    F.col("tt.end_org_id"),
    F.col("tt.end_org_name"),
    F.col("tt.order_num"),
    F.col("tt.driver1_emp_id"),
    F.col("tt.driver1_name"),
    F.col("tt.driver2_emp_id"),
    F.col("tt.driver2_name"),
    F.col("tt.truck_id"),
    F.col("tt.truck_no"),
    F.col("tt.actual_start_time"),
    F.col("tt.actual_end_time"),
    F.col("tt.actual_distance"),
    F.col("tt.finish_dur_sec"),
    # 分区字段固定为'20250721'（与原SQL保持一致）
    F.lit("20250713").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trans_trans_finish_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trans_trans_finish_inc")

# 8. 修复分区
repair_hive_table("dwd_trans_trans_finish_inc")



# ====================== 中转域入库事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_bound_inbound_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_bound_inbound_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_bound_inbound_inc (
    `id`             bigint COMMENT '中转记录ID',
    `order_id`       bigint COMMENT '运单ID',
    `org_id`         bigint COMMENT '机构ID',
    `inbound_time`   string COMMENT '入库时间',
    `inbound_emp_id` bigint COMMENT '入库人员'
) comment '中转域入库事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_bound_inbound_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理（读取ods_order_org_bound表）
# 修复后的入库时间及分区字段处理
inbound_df = spark.table("wl.ods_order_org_bound").filter(
    (F.col("dt") == "20250721") &
    (F.col("inbound_time").isNotNull())
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("org_id"),
    # 1. 修复 inbound_time：BIGINT→TIMESTAMP→UTC时间
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("inbound_time").cast(T.LongType()) / 1000),  # 关键：先转TIMESTAMP
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("inbound_time"),
    F.col("inbound_emp_id"),
    # 2. 修复分区字段 dt：同样需要先转TIMESTAMP
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("inbound_time").cast(T.LongType()) / 1000),  # 与上面保持一致
            "UTC"
        ),
        "yyyy-MM-dd"
    ).alias("dt")
)

# 5. 验证数据量（全量分区数据）
print_data_count(inbound_df, "dwd_bound_inbound_inc（全量分区）")

# 6. 写入全量分区数据
inbound_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_bound_inbound_inc")

# 7. 单独处理20250721分区数据
inbound_20250721_df = spark.table("wl.ods_order_org_bound").filter(
    F.col("dt") == "20250721"
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("org_id"),
    # 处理入库时间：UTC时间戳转字符串
    # 修复后的入库时间处理
    F.date_format(
        F.from_utc_timestamp(
            # 先将长整型时间戳转为 TIMESTAMP（若为毫秒级需除以1000）
            F.to_timestamp(F.col("inbound_time").cast(T.LongType()) / 1000),
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("inbound_time"),
    F.col("inbound_emp_id"),
    # 固定分区字段为20250721
    F.lit("20250713").alias("dt")
)

# 8. 验证20250721分区数据量
print_data_count(inbound_20250721_df, "dwd_bound_inbound_inc（20250721分区）")

# 9. 写入20250721分区数据（覆盖模式）
inbound_20250721_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_bound_inbound_inc")

# 10. 修复分区
repair_hive_table("dwd_bound_inbound_inc")


# ====================== 中转域分拣事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_bound_sort_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_bound_sort_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_bound_sort_inc (
    `id`            bigint COMMENT '中转记录ID',
    `order_id`      bigint COMMENT '订单ID',
    `org_id`        bigint COMMENT '机构ID',
    `sort_time`     string COMMENT '分拣时间',
    `sorter_emp_id` bigint COMMENT '分拣人员'
) comment '中转域分拣事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_bound_sort_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理（全量分区数据）
# 4.1 读取分拣记录并处理时间格式
# 修复后的分拣时间及分区字段处理
sort_full_df = spark.table("wl.ods_order_org_bound").filter(
    (F.col("dt") == "20250721") &
    (F.col("sort_time").isNotNull())
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("org_id"),
    # 1. 修复 sort_time：BIGINT→TIMESTAMP→UTC时间
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("sort_time").cast(T.LongType()) / 1000),  # 关键：先转 TIMESTAMP
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("sort_time"),
    F.col("sorter_emp_id"),
    # 2. 修复分区字段 dt：同样需要先转 TIMESTAMP
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("sort_time").cast(T.LongType()) / 1000),  # 与上面保持一致
            "UTC"
        ),
        "yyyy-MM-dd"
    ).alias("dt")
)

# 5. 验证全量分区数据量
print_data_count(sort_full_df, "dwd_bound_sort_inc（全量分区）")

# 6. 写入全量分区数据
sort_full_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_bound_sort_inc")

# 7. 单独处理20250721分区数据（带删除标记过滤）
sort_20250721_df = spark.table("wl.ods_order_org_bound").filter(
    (F.col("dt") == "20250721") &
    (F.col("sort_time").isNotNull())
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("org_id"),
    # 处理分拣时间：UTC时间戳转字符串
    # 修复后的分拣时间处理
    F.date_format(
        F.from_utc_timestamp(
            # 关键修复：先将 BIGINT 时间戳转为 TIMESTAMP（处理毫秒/秒级单位）
            F.to_timestamp(F.col("sort_time").cast(T.LongType()) / 1000),  # 若为毫秒级需÷1000，秒级可省略
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("sort_time"),
    F.col("sorter_emp_id"),
    # 固定分区字段为20250721
    F.lit("20250713").alias("dt")
)

# 8. 验证20250721分区数据量
print_data_count(sort_20250721_df, "dwd_bound_sort_inc（20250721分区）")

# 9. 写入20250721分区数据（覆盖模式）
sort_20250721_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_bound_sort_inc")

# 10. 修复分区
repair_hive_table("dwd_bound_sort_inc")


# ====================== 中转域出库事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_bound_outbound_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_bound_outbound_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_bound_outbound_inc (
    `id`              bigint COMMENT '中转记录ID',
    `order_id`        bigint COMMENT '订单ID',
    `org_id`          bigint COMMENT '机构ID',
    `outbound_time`   string COMMENT '出库时间',
    `outbound_emp_id` bigint COMMENT '出库人员'
) comment '中转域出库事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_bound_outbound_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理（全量分区数据）
# 4.1 读取出库记录并处理时间格式
outbound_full_df = spark.table("wl.ods_order_org_bound").filter(
    (F.col("dt") == "20250721") &
    (F.col("outbound_time").isNotNull())
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("org_id"),
    # 处理出库时间：修复后逻辑（正确）
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("outbound_time").cast(T.LongType()) / 1000),  # 毫秒→秒→TIMESTAMP
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("outbound_time"),
    F.col("outbound_emp_id"),
    # 修复分区字段 dt：与 outbound_time 保持一致的转换逻辑
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("outbound_time").cast(T.LongType()) / 1000),  # 关键：先转 TIMESTAMP
            "UTC"
        ),
        "yyyy-MM-dd"
    ).alias("dt")
)

# 5. 验证全量分区数据量
print_data_count(outbound_full_df, "dwd_bound_outbound_inc（全量分区）")

# 6. 写入全量分区数据
outbound_full_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_bound_outbound_inc")

# 7. 单独处理20250721分区数据（带删除标记过滤）
outbound_20250721_df = spark.table("wl.ods_order_org_bound").filter(
    (F.col("dt") == "20250721") &
    (F.col("outbound_time").isNotNull())
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("org_id"),
    # 处理出库时间：UTC时间戳转字符串
    # 修复后的出库时间处理
    F.date_format(
        F.from_utc_timestamp(
            # 先将 BIGINT 时间戳转为 TIMESTAMP（处理毫秒/秒级单位）
            F.to_timestamp(F.col("outbound_time").cast(T.LongType()) / 1000),  # 毫秒级→秒级（若为秒级可省略÷1000）
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("outbound_time"),
    F.col("outbound_emp_id"),
    # 固定分区字段为20250721
    F.lit("20250713").alias("dt")
)

# 8. 验证20250721分区数据量
print_data_count(outbound_20250721_df, "dwd_bound_outbound_inc（20250721分区）")

# 9. 写入20250721分区数据（覆盖模式）
outbound_20250721_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_bound_outbound_inc")

# 10. 修复分区
repair_hive_table("dwd_bound_outbound_inc")




# ====================== 交易域支付成功事务事实表 ======================
# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/tms_pyspark/dwd/dwd_trade_pay_suc_detail_inc")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS tms_spark_dwd.dwd_trade_pay_suc_detail_inc")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE tms_spark_dwd.dwd_trade_pay_suc_detail_inc (
    `id`                   bigint comment '运单明细ID',
    `order_id`             string COMMENT '运单ID',
    `cargo_type`           string COMMENT '货物类型ID',
    `cargo_type_name`      string COMMENT '货物类型名称',
    `volumn_length`        bigint COMMENT '长cm',
    `volumn_width`         bigint COMMENT '宽cm',
    `volumn_height`        bigint COMMENT '高cm',
    `weight`               decimal(16, 2) COMMENT '重量 kg',
    `payment_time`         string COMMENT '支付时间',
    `order_no`             string COMMENT '运单号',
    `status`               string COMMENT '运单状态',
    `status_name`          string COMMENT '运单状态名称',
    `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
    `collect_type_name`    string COMMENT '取件类型名称',
    `user_id`              bigint COMMENT '用户ID',
    `receiver_complex_id`  bigint COMMENT '收件人小区id',
    `receiver_province_id` string COMMENT '收件人省份id',
    `receiver_city_id`     string COMMENT '收件人城市id',
    `receiver_district_id` string COMMENT '收件人区县id',
    `receiver_name`        string COMMENT '收件人姓名',
    `sender_complex_id`    bigint COMMENT '发件人小区id',
    `sender_province_id`   string COMMENT '发件人省份id',
    `sender_city_id`       string COMMENT '发件人城市id',
    `sender_district_id`   string COMMENT '发件人区县id',
    `sender_name`          string COMMENT '发件人姓名',
    `payment_type`         string COMMENT '支付方式',
    `payment_type_name`    string COMMENT '支付方式名称',
    `cargo_num`            bigint COMMENT '货物个数',
    `amount`               decimal(16, 2) COMMENT '金额',
    `estimate_arrive_time` string COMMENT '预计到达时间',
    `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
) comment '交易域支付成功事务事实表'
PARTITIONED BY (`dt` string comment '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dwd/dwd_trade_pay_suc_detail_inc'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 4. 数据处理
# 4.1 读取货物信息（ods_order_cargo）
cargo_df = spark.table("wl.ods_order_cargo").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(
    F.col("id"),
    F.col("order_id"),
    F.col("cargo_type"),
    F.col("volume_length"),
    F.col("volume_width"),
    F.col("volume_height"),
    F.col("weight")
)

# 4.2 读取订单信息（ods_order_info）并处理支付时间
order_info_df = spark.table("wl.ods_order_info").filter(
    (F.col("dt") == "20250721") &
    (F.col("is_deleted") == "0") &
    ~F.col("status").isin("60010", "60999")  # 排除特定状态
).select(
    F.col("id"),
    F.col("order_no"),
    F.col("status"),
    F.col("collect_type"),
    F.col("user_id"),
    F.col("receiver_complex_id"),
    F.col("receiver_province_id"),
    F.col("receiver_city_id"),
    F.col("receiver_district_id"),
    # 收件人姓名脱敏
    F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
    F.col("sender_complex_id"),
    F.col("sender_province_id"),
    F.col("sender_city_id"),
    F.col("sender_district_id"),
    # 发件人姓名脱敏
    F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
    F.col("payment_type"),
    F.col("cargo_num"),
    F.col("amount"),
    # 修复：预计到达时间（BIGINT→TIMESTAMP→UTC时间）
    F.date_format(
        F.from_utc_timestamp(
            F.to_timestamp(F.col("estimate_arrive_time").cast(T.LongType()) / 1000),  # 毫秒→秒→TIMESTAMP
            "UTC"
        ),
        "yyyy-MM-dd HH:mm:ss"
    ).alias("estimate_arrive_time"),
    F.col("distance"),
    # 处理支付时间（从update_time提取）
    F.concat(
        F.substring(F.col("update_time"), 1, 10),
        F.lit(" "),
        F.substring(F.col("update_time"), 12, 8)
    ).alias("payment_time")
)

# 4.3-4.6 读取字典表（ods_base_dic）
cargo_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("cargo_type_name"))

status_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("status_name"))

collect_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("collect_type_name"))

payment_type_dic = spark.table("wl.ods_base_dic").filter(
    (F.col("dt") == "20250721") & (F.col("is_deleted") == "0")
).select(F.col("id"), F.col("name").alias("payment_type_name"))

# 5. 多表关联
joined_df = cargo_df.alias("c") \
    .join(order_info_df.alias("oi"), F.col("c.order_id") == F.col("oi.id"), "inner") \
    .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
    .join(status_dic.alias("s"), F.col("oi.status") == F.col("s.id").cast(T.StringType()), "left") \
    .join(collect_type_dic.alias("colt"), F.col("oi.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
    .join(payment_type_dic.alias("pt"), F.col("oi.payment_type") == F.col("pt.id").cast(T.StringType()), "left") \
    .select(
    F.col("c.id"),
    F.col("c.order_id"),
    F.col("c.cargo_type"),
    F.col("ct.cargo_type_name"),
    F.col("c.volume_length"),
    F.col("c.volume_width"),
    F.col("c.volume_height"),
    F.col("c.weight"),
    F.col("oi.payment_time"),
    F.col("oi.order_no"),
    F.col("oi.status"),
    F.col("s.status_name"),
    F.col("oi.collect_type"),
    F.col("colt.collect_type_name"),
    F.col("oi.user_id"),
    F.col("oi.receiver_complex_id"),
    F.col("oi.receiver_province_id"),
    F.col("oi.receiver_city_id"),
    F.col("oi.receiver_district_id"),
    F.col("oi.receiver_name"),
    F.col("oi.sender_complex_id"),
    F.col("oi.sender_province_id"),
    F.col("oi.sender_city_id"),
    F.col("oi.sender_district_id"),
    F.col("oi.sender_name"),
    F.col("oi.payment_type"),
    F.col("pt.payment_type_name"),
    F.col("oi.cargo_num"),
    F.col("oi.amount"),
    F.col("oi.estimate_arrive_time"),
    F.col("oi.distance"),
    # 分区字段：支付时间的日期部分
    F.date_format(F.col("oi.payment_time"), "yyyy-MM-dd").alias("dt")
)

# 6. 验证数据量
print_data_count(joined_df, "dwd_trade_pay_suc_detail_inc")

# 7. 写入数据
joined_df.write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dwd/dwd_trade_pay_suc_detail_inc")

# 8. 修复分区
repair_hive_table("dwd_trade_pay_suc_detail_inc")