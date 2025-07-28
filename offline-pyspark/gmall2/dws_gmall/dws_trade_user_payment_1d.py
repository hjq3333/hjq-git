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
   `user_id`           STRING COMMENT '用户ID',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '互动域商品粒度收藏商品最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS orc
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dws/dws_trade_user_payment_1d';
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
        .insertInto(f"dws.{tableName}")


# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    # 构建SQL语句，修正字段别名以匹配Hive表结构
    select_sql = f"""
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount),
    dt
from dwd.dwd_trade_pay_detail_suc_inc
group by user_id,dt;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))
    df_with_partition = df_with_partition.drop("ds")

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)


# 4. 主函数（示例调用）
if __name__ == "__main__":
    table_name = 'dws_trade_user_payment_1d'
    # 设置目标分区日期
    target_date = '2025-07-17'
    execute_hive_table_creation(table_name)
    # 执行插入操作
    execute_hive_insert(target_date, 'dws_trade_user_payment_1d')