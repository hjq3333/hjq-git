
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

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

# 使用insertInto方法写入已存在的分区表
def select_to_hive(df, tableName, partition_date):
    # 确保 order_continuously_user_count 列存在并且是 BIGINT 类型
    if "order_continuously_user_count" in df.columns:
        df = df.withColumn("order_continuously_user_count", col("order_continuously_user_count").cast("BIGINT"))
    df.write.mode('append').insertInto(f"ads.{tableName}")

# 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    # 构建SQL语句，修正字段别名以匹配Hive表结构
    query1 = """
    SELECT '2025-06-30' AS dt, 7 AS recent_days, COUNT(DISTINCT user_id) AS order_continuously_user_count
    FROM (
        SELECT user_id, DATEDIFF(LEAD(dt, 2, '9999-12-31') OVER (PARTITION BY user_id ORDER BY dt), dt) AS diff
        FROM dws.dws_trade_user_order_1d
        WHERE dt >= DATE_ADD('2025-06-30', -6)
    ) t1
    WHERE diff = 2;
    """

    query2 = """
    SELECT '2025-06-30' AS dt, 7 AS recent_days, COUNT(DISTINCT user_id) AS order_continuously_user_count
    FROM (
        SELECT user_id
        FROM (
            SELECT user_id, DATE_SUB(dt, RANK() OVER (PARTITION BY user_id ORDER BY dt)) AS diff
            FROM dws.dws_trade_user_order_1d
            WHERE dt >= DATE_ADD('2025-06-30', -6)
        ) t1
        GROUP BY user_id, diff
        HAVING COUNT(*) >= 3
    ) t2;
    """

    query3 = """
    SELECT '2025-06-30' AS dt, 7 AS recent_days, COUNT(*) AS order_continuously_user_count
    FROM (
        SELECT user_id, SUM(num) AS s
        FROM (
            SELECT user_id,
                CASE dt
                    WHEN '2025-06-30' THEN 1
                    WHEN '2025-06-29' THEN 10
                    WHEN '2025-06-28' THEN 100
                    WHEN '2025-06-27' THEN 1000
                    WHEN '2025-06-26' THEN 10000
                    WHEN '2025-06-25' THEN 100000
                    WHEN '2025-06-24' THEN 1000000
                    ELSE 0
                END AS num
            FROM dws.dws_trade_user_order_1d
            WHERE dt >= DATE_ADD('2025-06-30', -6)
        ) t1
        GROUP BY user_id
    ) t2
    WHERE CAST(s AS STRING) LIKE '%111%';
    """

    query4 = """
    SELECT '2025-06-30' AS dt, 7 AS recent_days, COUNT(DISTINCT user_id) AS order_continuously_user_count
    FROM (
        SELECT user_id, DATEDIFF(LEAD(dt, 2, '9999-12-31') OVER (PARTITION BY user_id ORDER BY dt), dt) AS diff
        FROM dws.dws_trade_user_order_1d
        WHERE dt >= DATE_ADD('2025-06-30', -6)
    ) t1
    WHERE diff <= 3;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(query1)
    df2 = spark.sql(query2)
    df3 = spark.sql(query3)
    df4 = spark.sql(query4)

    # 合并结果
    df = df1.unionByName(df2).unionByName(df3).unionByName(df4)

    # 添加分区字段
    df_with_partition = df.withColumn("ds", lit(partition_date))
    df_with_partition = df_with_partition.drop("ds")

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)

# 主函数（示例调用）
if __name__ == "__main__":
    # 设置目标分区日期
    target_date = '2025-06-27'

    # 执行插入操作
    execute_hive_insert(target_date, 'ads_order_continuously_user_count')
