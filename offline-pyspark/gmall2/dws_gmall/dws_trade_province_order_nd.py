from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# 1. 初始化SparkSession（全局单例模式）
def get_spark_session():
    """创建并返回启用Hive支持的SparkSession"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE gmall")
    return spark

def execute_hive_table_creation(tableName):
    spark = get_spark_session()

    create_table_sql = f"""
DROP TABLE IF EXISTS {tableName};
CREATE EXTERNAL TABLE {tableName}
(
    `province_id`                STRING COMMENT '省份ID',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                 STRING COMMENT '新版国际标准地区编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_trade_province_order_nd';
    """

    print(f"[INFO] 开始创建表: {tableName}")
    # 执行多条SQL语句
    for sql in create_table_sql.strip().split(';'):
        if sql.strip():
            spark.sql(sql)
    print(f"[INFO] 表 {tableName} 创建成功")

def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"gmall.{tableName}")


# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    # 构建SQL语句，修正字段别名以匹配Hive表结构
    select_sql = f"""
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('2025-06-30',-6),order_count_1d,0)),
    sum(if(dt>=date_add('2025-06-30',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('2025-06-30',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2025-06-30',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('2025-06-30',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_province_order_1d
where dt>=date_add('2025-06-30',-29)
  and dt<='2025-06-30'
group by province_id,province_name,area_code,iso_code,iso_3166_2;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 4. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dws_trade_province_order_nd'
    # 设置目标分区日期
    target_date = '2025-07-17'
    execute_hive_table_creation(table_name)
    # 执行插入操作
    execute_hive_insert(target_date, 'dws_trade_province_order_nd')