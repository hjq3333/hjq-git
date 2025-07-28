
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col


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
    ui.user_id,
    date_format(ui.create_time,'yyyy-MM-dd') date_id,
    ui.create_time,
    log.channel,
    log.province_id,
    log.version_code,
    log.mid_id,
    log.brand,
    log.model,
    log.operate_system
from
    (
        select
            data.id as user_id,
            data.create_time
        from ods_user_info data
        where dt='20250630'

    ) ui
        left join
    (
        select
            get_json_object(log, '$.common.ar') as province_id,  -- 修改此处，使用正确的列名log
            get_json_object(log, '$.common.ba') as brand,
            get_json_object(log, '$.common.ch') as channel,
            get_json_object(log, '$.common.md') as model,
            get_json_object(log, '$.common.mid') as mid_id,
            get_json_object(log, '$.common.os') as operate_system,
            get_json_object(log, '$.common.uid') as user_id,
            get_json_object(log, '$.common.vc') as version_code
        from ods_z_log
        where dt='20250630'
          and get_json_object(log, '$.page.page_id') = 'register'  -- 修改此处，使用正确的列名log
          and get_json_object(log, '$.common.uid') is not null
    ) log
    on ui.user_id = log.user_id;
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
    # 设置目标分区日期
    target_date = '2025-06-27'

    # 执行插入操作
    execute_hive_insert(target_date, 'dwd_user_register_inc')