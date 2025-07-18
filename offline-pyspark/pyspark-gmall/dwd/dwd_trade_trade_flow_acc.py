from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf, col
from pyspark.sql import functions as F


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


def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"dwd.{tableName}")



# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    # 构建SQL语句，修正字段别名以匹配Hive表结构
    select_sql = f"""
select
        oi.id as order_id,
        user_id,
        province_id,
        date_format(create_time,'yyyy-MM-dd') as order_date_id,
        create_time as order_time,
        date_format(callback_time,'yyyy-MM-dd') as payment_date_id,
        callback_time as payment_time,
        date_format(finish_time,'yyyy-MM-dd') as finish_date_id,
        finish_time,
        original_total_amount as order_original_amount,
        activity_reduce_amount as order_activity_amount,
        coupon_reduce_amount as order_coupon_amount,
        total_amount as order_total_amount,
        nvl(payment_amount, 0.0) as payment_amount
    from
        (
            select
                data.id,
                data.user_id,
                data.province_id,
                data.create_time,
                data.original_total_amount,
                data.activity_reduce_amount,
                data.coupon_reduce_amount,
                data.total_amount
            from ods_order_info data
            where dt='20250630'
        ) oi
        left join
        (
            select
                data.order_id,
                data.callback_time,
                data.total_amount as payment_amount
            from ods_payment_info data
            where dt='20250630'
        ) pi
        on oi.id = pi.order_id
        left join
        (
            select
                data.order_id,
                data.operate_time as finish_time
            from ods_order_status_log data
            where dt='20250630'
              and data.order_status='1004'
        ) log
        on oi.id = log.order_id;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)

    # 添加分区字段
    df_with_partition = df1.withColumn("ds", lit(partition_date))
    df_with_partition = df_with_partition.withColumn("payment_amount", F.col("payment_amount").cast("DECIMAL(16,2)"))
    df_with_partition = df_with_partition.drop("date_format(create_time, yyyy-MM-dd)").drop("dt")

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)



# 4. 主函数（示例调用）
if __name__ == "__main__":
    # 设置目标分区日期
    target_date = '2025-07-17'

    # 执行插入操作
    execute_hive_insert(target_date, 'dwd_trade_trade_flow_acc')