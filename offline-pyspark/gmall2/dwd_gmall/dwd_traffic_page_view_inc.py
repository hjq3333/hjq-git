
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
    common.province_id,
    common.brand,
    common.channel,
    common.is_new,
    common.model,
    common.mid_id,
    common.operate_system,
    common.user_id,
    common.version_code,
    page_data.item as page_item,
    page_data.item_type as page_item_type,
    page_data.last_page_id,
    page_data.page_id,
    page_data.from_pos_id,
    page_data.from_pos_seq,
    page_data.refer_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    common.session_id,
    page_data.during_time
from (
         select
             get_json_object(log, '$.common') as common_json,
             get_json_object(log, '$.page') as page_json,
             get_json_object(log, '$.ts') as ts
         from ods_z_log
         where dt='20250630'
     ) base
         lateral view json_tuple(common_json, 'ar', 'ba', 'ch', 'is_new', 'md', 'mid', 'os', 'uid', 'vc', 'sid') common as province_id, brand, channel, is_new, model, mid_id, operate_system, user_id, version_code, session_id
         lateral view json_tuple(page_json, 'item', 'item_type', 'last_page_id', 'page_id', 'from_pos_id', 'from_pos_seq', 'refer_id', 'during_time') page_data as item, item_type, last_page_id, page_id, from_pos_id, from_pos_seq, refer_id, during_time
where page_json is not null;
    """

    # 执行SQL
    print(f"[INFO] 开始执行SQL插入，分区日期：{partition_date}")
    df1 = spark.sql(select_sql)
    df_with_partition = df1.withColumn("during_time", col("during_time").cast("BIGINT"))

    # 添加分区字段
    df_with_partition = df_with_partition.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show()

    # 写入Hive
    select_to_hive(df_with_partition, tableName, partition_date)







# 4. 主函数（示例调用）
if __name__ == "__main__":
    # 设置目标分区日期
    target_date = '2025-06-27'

    # 执行插入操作
    execute_hive_insert(target_date, 'dwd_traffic_page_view_inc')