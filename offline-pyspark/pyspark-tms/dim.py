from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("WL_Dimension_Tables") \
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

spark.sql("USE tms01")

def create_hdfs_dir(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

# 新增：修复Hive表分区的函数（关键）
def repair_hive_table(table_name):
    spark.sql(f"MSCK REPAIR TABLE tms.{table_name}")
    print(f"修复分区完成：tms.{table_name}")

# 新增：打印数据量的函数（验证数据是否存在）
def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count


# ====================== 1. 小区维度表 dim_complex_full ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_complex_full")
spark.sql("DROP TABLE IF EXISTS tms.dim_complex_full")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_complex_full (
    id BIGINT COMMENT '小区ID',
    complex_name STRING COMMENT '小区名称',
    courier_emp_ids ARRAY<STRING> COMMENT '负责快递员IDS',
    province_id BIGINT COMMENT '省份ID',
    province_name STRING COMMENT '省份名称',
    city_id BIGINT COMMENT '城市ID',
    city_name STRING COMMENT '城市名称',
    district_id BIGINT COMMENT '区（县）ID',
    district_name STRING COMMENT '区（县）名称'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_complex_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

complex_info = spark.table("tms01.ods_base_complex").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "complex_name", "province_id", "city_id", "district_id", "district_name")

dic_prov = spark.table("tms01.ods_base_region_info").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name").withColumnRenamed("id", "province_id").withColumnRenamed("name", "province_name")

dic_city = spark.table("tms01.ods_base_region_info").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name").withColumnRenamed("id", "city_id").withColumnRenamed("name", "city_name")

complex_courier = spark.table("tms01.ods_express_courier_complex").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).groupBy("complex_id").agg(
    F.collect_set(F.col("courier_emp_id").cast(T.StringType())).alias("courier_emp_ids")
)

joined = complex_info \
    .join(dic_prov, "province_id", "inner") \
    .join(dic_city, "city_id", "inner") \
    .join(complex_courier, complex_info.id == complex_courier.complex_id, "left") \
    .select(
    "id", "complex_name",
    F.coalesce("courier_emp_ids", F.array()).alias("courier_emp_ids"),
    "province_id", "province_name", "city_id", "city_name", "district_id", "district_name"
)

# 验证数据量
print_data_count(joined, "dim_complex_full")

# 写入数据
joined.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_complex_full")

# 修复分区
repair_hive_table("dim_complex_full")


# ====================== 2. 机构维度表 dim_organ_full ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_organ_full")
spark.sql("DROP TABLE IF EXISTS tms.dim_organ_full")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_organ_full (
    id BIGINT COMMENT '机构ID',
    org_name STRING COMMENT '机构名称',
    org_level BIGINT COMMENT '机构等级（1为转运中心，2为转运站）',
    region_id BIGINT COMMENT '地区ID，1级机构为city ,2级机构为district',
    region_name STRING COMMENT '地区名称',
    region_code STRING COMMENT '地区编码（行政级别）',
    org_parent_id BIGINT COMMENT '父级机构ID',
    org_parent_name STRING COMMENT '父级机构名称'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_organ_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

organ_info = spark.table("tms01.ods_base_organ").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "org_name", "org_level", "region_id", "org_parent_id").alias("o")

region_info = spark.table("tms01.ods_base_region_info").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name", "dict_code").withColumnRenamed("name", "region_name").withColumnRenamed("dict_code", "region_code").alias("r")

org_parent = spark.table("tms01.ods_base_organ").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "org_name").withColumnRenamed("id", "org_parent_id").withColumnRenamed("org_name", "org_parent_name").alias("op")

joined = organ_info \
    .join(region_info, organ_info.region_id == region_info.id, "left") \
    .join(org_parent, organ_info.org_parent_id == org_parent.org_parent_id, "left") \
    .select(
    organ_info.id, "org_name", "org_level",
    "region_id", F.coalesce(region_info.region_name, F.lit("")).alias("region_name"),
    F.coalesce(region_info.region_code, F.lit("")).alias("region_code"),
    organ_info.org_parent_id, F.coalesce(org_parent.org_parent_name, F.lit("")).alias("org_parent_name")
)

print_data_count(joined, "dim_organ_full")

joined.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_organ_full")

repair_hive_table("dim_organ_full")


# ====================== 3. 地区维度表 dim_region_full ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_region_full")
spark.sql("DROP TABLE IF EXISTS tms.dim_region_full")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_region_full (
    id BIGINT COMMENT '地区ID',
    parent_id BIGINT COMMENT '上级地区ID',
    name STRING COMMENT '地区名称',
    dict_code STRING COMMENT '编码（行政级别）',
    short_name STRING COMMENT '简称'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_region_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

region_df = spark.table("tms01.ods_base_region_info").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "parent_id", "name", "dict_code", "short_name")

print_data_count(region_df, "dim_region_full")

region_df.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_region_full")

repair_hive_table("dim_region_full")


# ====================== 4. 快递员维度表 dim_express_courier_full ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_express_courier_full")
spark.sql("DROP TABLE IF EXISTS tms.dim_express_courier_full")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_express_courier_full (
    id BIGINT COMMENT '快递员ID',
    emp_id BIGINT COMMENT '员工ID',
    org_id BIGINT COMMENT '所属机构ID',
    org_name STRING COMMENT '机构名称',
    working_phone STRING COMMENT '工作电话',
    express_type STRING COMMENT '快递员类型（收货；发货）',
    express_type_name STRING COMMENT '快递员类型名称'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_express_courier_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

courier_info = spark.table("tms01.ods_express_courier").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "emp_id", "org_id", "working_phone", "express_type").alias("c")

org_info = spark.table("tms01.ods_base_organ").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "org_name").withColumnRenamed("id", "org_id").alias("o")

dic_info = spark.table("tms01.ods_base_dic").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name").withColumnRenamed("id", "express_type").withColumnRenamed("name", "express_type_name").alias("d")

joined = courier_info \
    .join(org_info, courier_info.org_id == org_info.org_id, "inner") \
    .join(dic_info, courier_info.express_type == dic_info.express_type, "inner") \
    .select(
    courier_info.id, "emp_id", org_info.org_id, "org_name",
    F.md5(courier_info.working_phone).alias("working_phone"),
    dic_info.express_type, "express_type_name"
)

print_data_count(joined, "dim_express_courier_full")

joined.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_express_courier_full")

repair_hive_table("dim_express_courier_full")


# ====================== 5. 班次维度表 dim_shift_full ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_shift_full")
spark.sql("DROP TABLE IF EXISTS tms.dim_shift_full")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_shift_full (
    id BIGINT COMMENT '班次ID',
    line_id BIGINT COMMENT '线路ID',
    line_name STRING COMMENT '线路名称',
    line_no STRING COMMENT '线路编号',
    line_level STRING COMMENT '线路级别',
    org_id BIGINT COMMENT '所属机构',
    transport_line_type_id STRING COMMENT '线路类型ID',
    transport_line_type_name STRING COMMENT '线路类型名称',
    start_org_id BIGINT COMMENT '起始机构ID',
    start_org_name STRING COMMENT '起始机构名称',
    end_org_id BIGINT COMMENT '目标机构ID',
    end_org_name STRING COMMENT '目标机构名称',
    pair_line_id BIGINT COMMENT '配对线路ID',
    distance DECIMAL(10,2) COMMENT '直线距离',
    cost DECIMAL(10,2) COMMENT '公路里程',
    estimated_time BIGINT COMMENT '预计时间（分钟）',
    start_time STRING COMMENT '班次开始时间',
    driver1_emp_id BIGINT COMMENT '第一司机',
    driver2_emp_id BIGINT COMMENT '第二司机',
    truck_id BIGINT COMMENT '卡车ID',
    pair_shift_id BIGINT COMMENT '配对班次(同一辆车一去一回的另一班次)'
) 
PARTITIONED BY (dt STRING COMMENT '统计周期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_shift_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

shift_info = spark.table("tms01.ods_line_base_shift").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "line_id", "start_time", "driver1_emp_id", "driver2_emp_id", "truck_id", "pair_shift_id").alias("s")

line_info = spark.table("tms01.ods_line_base_info").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name", "line_no", "line_level", "org_id", "transport_line_type_id",
         "start_org_id", "start_org_name", "end_org_id", "end_org_name",
         "pair_line_id", "distance", "cost", "estimated_time").alias("l")

dic_type = spark.table("tms01.ods_base_dic").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name").withColumnRenamed("id", "transport_line_type_id").withColumnRenamed("name", "transport_line_type_name").alias("d")

joined = shift_info \
    .join(line_info, shift_info.line_id == line_info.id, "inner") \
    .join(dic_type, line_info.transport_line_type_id == dic_type.transport_line_type_id, "inner") \
    .select(
    shift_info.id, "line_id", line_info.name, "line_no", "line_level", line_info.org_id,
    dic_type.transport_line_type_id, "transport_line_type_name",
    "start_org_id", "start_org_name", "end_org_id", "end_org_name",
    "pair_line_id", "distance", "cost", "estimated_time",
    "start_time", "driver1_emp_id", "driver2_emp_id", "truck_id", "pair_shift_id"
).withColumnRenamed("name", "line_name")

print_data_count(joined, "dim_shift_full")

joined.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_shift_full")

repair_hive_table("dim_shift_full")


# ====================== 6. 司机维度表 dim_truck_driver_full ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_truck_driver_full")
spark.sql("DROP TABLE IF EXISTS tms.dim_truck_driver_full")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_truck_driver_full (
    id BIGINT COMMENT '司机信息ID',
    emp_id BIGINT COMMENT '员工ID',
    org_id BIGINT COMMENT '所属机构ID',
    org_name STRING COMMENT '所属机构名称',
    team_id BIGINT COMMENT '所属车队ID',
    tream_name STRING COMMENT '所属车队名称',
    license_type STRING COMMENT '准驾车型',
    init_license_date STRING COMMENT '初次领证日期',
    expire_date STRING COMMENT '有效截止日期',
    license_no STRING COMMENT '驾驶证号',
    is_enabled TINYINT COMMENT '状态 0：禁用 1：正常'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_truck_driver_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

driver_info = spark.table("tms01.ods_truck_driver").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "emp_id", "org_id", "team_id", "license_type",
         "init_license_date", "expire_date", "license_no", "is_enabled").alias("d")

org_info = spark.table("tms01.ods_base_organ").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "org_name").withColumnRenamed("id", "org_id").alias("o")

team_info = spark.table("tms01.ods_truck_team").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name").withColumnRenamed("id", "team_id").withColumnRenamed("name", "tream_name").alias("t")

joined = driver_info \
    .join(org_info, "org_id", "inner") \
    .join(team_info, "team_id", "inner") \
    .select(
    driver_info.id, "emp_id", driver_info.org_id, "org_name",
    "team_id", "tream_name", "license_type",
    "init_license_date", "expire_date", "license_no", "is_enabled"
)

print_data_count(joined, "dim_truck_driver_full")

joined.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_truck_driver_full")

repair_hive_table("dim_truck_driver_full")


# ====================== 7. 卡车维度表 dim_truck_full ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_truck_full")
spark.sql("DROP TABLE IF EXISTS tms.dim_truck_full")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_truck_full (
    id BIGINT COMMENT '卡车ID',
    team_id BIGINT COMMENT '所属车队ID',
    team_name STRING COMMENT '所属车队名称',
    team_no STRING COMMENT '车队编号',
    org_id BIGINT COMMENT '所属机构',
    org_name STRING COMMENT '所属机构名称',
    manager_emp_id BIGINT COMMENT '负责人',
    truck_no STRING COMMENT '车牌号码',
    truck_model_id STRING COMMENT '型号',
    truck_model_name STRING COMMENT '型号名称',
    truck_model_type STRING COMMENT '型号类型',
    truck_model_type_name STRING COMMENT '型号类型名称',
    truck_model_no STRING COMMENT '型号编码',
    truck_brand STRING COMMENT '品牌',
    truck_brand_name STRING COMMENT '品牌名称',
    truck_weight DECIMAL(16,2) COMMENT '整车重量（吨）',
    load_weight DECIMAL(16,2) COMMENT '额定载重（吨）',
    total_weight DECIMAL(16,2) COMMENT '总质量（吨）',
    eev STRING COMMENT '排放标准',
    boxcar_len DECIMAL(16,2) COMMENT '货箱长（m）',
    boxcar_wd DECIMAL(16,2) COMMENT '货箱宽（m）',
    boxcar_hg DECIMAL(16,2) COMMENT '货箱高（m）',
    max_speed BIGINT COMMENT '最高时速（千米/时）',
    oil_vol BIGINT COMMENT '油箱容积（升）',
    device_gps_id STRING COMMENT 'GPS设备ID',
    engine_no STRING COMMENT '发动机编码',
    license_registration_date STRING COMMENT '注册时间',
    license_last_check_date STRING COMMENT '最后年检日期',
    license_expire_date STRING COMMENT '失效日期',
    is_enabled TINYINT COMMENT '状态 0：禁用 1：正常'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_truck_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

truck_info = spark.table("tms01.ods_truck_info").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "team_id", "truck_model_id", "device_gps_id",
         "engine_no", "license_registration_date", "license_last_check_date",
         "license_expire_date", "is_enabled", "truck_no").alias("t")

team_info = spark.table("tms01.ods_truck_team").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name", "team_no", "org_id", "manager_emp_id").withColumnRenamed("id", "team_id").withColumnRenamed("name", "team_name").alias("tm")

model_info = spark.table("tms01.ods_truck_model").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "model_name", "model_type", "model_no", "brand",
         "truck_weight", "load_weight", "total_weight", "eev",
         "boxcar_len", "boxcar_wd", "boxcar_hg", "max_speed", "oil_vol").alias("m")

org_info = spark.table("tms01.ods_base_organ").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "org_name").withColumnRenamed("id", "org_id").alias("o")

dic_type = spark.table("tms01.ods_base_dic").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name").withColumnRenamed("id", "model_type").withColumnRenamed("name", "truck_model_type_name").alias("dt")

dic_brand = spark.table("tms01.ods_base_dic").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "name").withColumnRenamed("id", "brand").withColumnRenamed("name", "truck_brand_name").alias("db")

joined = truck_info \
    .join(team_info, "team_id", "inner") \
    .join(model_info, truck_info.truck_model_id == model_info.id, "inner") \
    .join(org_info, team_info.org_id == org_info.org_id, "inner") \
    .join(dic_type, model_info.model_type == dic_type.model_type, "inner") \
    .join(dic_brand, model_info.brand == dic_brand.brand, "inner") \
    .select(
    truck_info.id, "team_id", "team_name", "team_no", team_info.org_id, "org_name", "manager_emp_id",
    F.md5(truck_info.truck_no).alias("truck_no"),
    "truck_model_id", "model_name", model_info.model_type, "truck_model_type_name",
    "model_no", model_info.brand, "truck_brand_name",
    "truck_weight", "load_weight", "total_weight", "eev",
    "boxcar_len", "boxcar_wd", "boxcar_hg", "max_speed", "oil_vol",
    "device_gps_id", "engine_no", "license_registration_date",
    "license_last_check_date", "license_expire_date", "is_enabled"
).withColumnRenamed("model_name", "truck_model_name")

print_data_count(joined, "dim_truck_full")

joined.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_truck_full")

repair_hive_table("dim_truck_full")


# ====================== 8. 用户拉链表 dim_user_zip ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_user_zip")
spark.sql("DROP TABLE IF EXISTS tms.dim_user_zip")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_user_zip (
    id BIGINT COMMENT '用户地址信息ID',
    login_name STRING COMMENT '用户名称',
    nick_name STRING COMMENT '用户昵称',
    passwd STRING COMMENT '用户密码',
    real_name STRING COMMENT '用户姓名',
    phone_num STRING COMMENT '手机号',
    email STRING COMMENT '邮箱',
    user_level STRING COMMENT '用户级别',
    birthday STRING COMMENT '用户生日',
    gender STRING COMMENT '性别 M男,F女',
    start_date STRING COMMENT '起始日期',
    end_date STRING COMMENT '结束日期'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_user_zip'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

user_info = spark.table("tms01.ods_user_info").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "login_name", "nick_name", "passwd", "real_name",
         "phone_num", "email", "user_level", "birthday", "gender", "create_time")

user_df = user_info \
    .withColumn("passwd", F.md5(user_info.passwd).alias("passwd")) \
    .withColumn("real_name", F.md5(user_info.real_name).alias("real_name")) \
    .withColumn("phone_num", F.when(
    F.col("phone_num").rlike(r'^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\d{8}$'),
    F.md5(user_info.phone_num)
).otherwise(None)) \
    .withColumn("email", F.when(
    F.col("email").rlike(r'^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+$'),
    F.md5(user_info.email)
).otherwise(None)) \
    .withColumn("birthday", F.expr("date_add('1970-01-01', cast(birthday as int))")) \
    .withColumn("start_date", F.from_unixtime(user_info.create_time / 1000).cast(T.DateType())) \
    .withColumn("end_date", F.lit("9999-12-31")) \
    .drop("create_time")

print_data_count(user_df, "dim_user_zip")

user_df.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_user_zip")

repair_hive_table("dim_user_zip")


# ====================== 9. 用户地址拉链表 dim_user_address_zip ======================
create_hdfs_dir("/warehouse/tms_pyspark/dim/dim_user_address_zip")
spark.sql("DROP TABLE IF EXISTS tms.dim_user_address_zip")
spark.sql("""
CREATE EXTERNAL TABLE tms.dim_user_address_zip (
    id BIGINT COMMENT '地址ID',
    user_id BIGINT COMMENT '用户ID',
    phone STRING COMMENT '电话号', 
    province_id BIGINT COMMENT '所属省份ID',
    city_id BIGINT COMMENT '所属城市ID',
    district_id BIGINT COMMENT '所属区县ID',
    complex_id BIGINT COMMENT '所属小区ID',
    address STRING COMMENT '详细地址',
    is_default TINYINT COMMENT '是否默认',
    start_date STRING COMMENT '起始日期',
    end_date STRING COMMENT '结束日期'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/tms_pyspark/dim/dim_user_address_zip'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

addr_info = spark.table("tms01.ods_user_address").filter(
    (F.col("dt") == "20250712") & (F.col("is_deleted") == "0")
).select("id", "user_id", "phone", "province_id", "city_id",
         "district_id", "complex_id", "address", "is_default", "create_time")

addr_df = addr_info \
    .withColumn("phone", F.when(
    F.col("phone").rlike(r'^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\d{8}$'),
    F.md5(addr_info.phone)
).otherwise(None)) \
    .withColumn("start_date", F.from_unixtime(addr_info.create_time / 1000).cast(T.DateType())) \
    .withColumn("end_date", F.lit("9999-12-31")) \
    .drop("create_time")

print_data_count(addr_df, "dim_user_address_zip")

addr_df.withColumn("dt", F.lit("20250712")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/tms_pyspark/dim/dim_user_address_zip")

repair_hive_table("dim_user_address_zip")


spark.stop()




