
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
    user_id,
    date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd') as date_id,  -- 日期ID
    date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as login_time,  -- 登录时间
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from (
         select
             common_uid as user_id,
             common_ch as channel,
             common_ar as province_id,
             common_vc as version_code,
             common_mid as mid_id,
             common_ba as brand,
             common_md as model,
             common_os as operate_system,
             ts,
             row_number() over (partition by common_sid order by ts) as rn  -- 按 session 去重
         from (
                  select
                      -- 解析 JSON 字段（假设日志存在 log 列）
                      get_json_object(log, '$.common.uid') as common_uid,
                      get_json_object(log, '$.common.ch') as common_ch,
                      get_json_object(log, '$.common.ar') as common_ar,
                      get_json_object(log, '$.common.vc') as common_vc,
                      get_json_object(log, '$.common.mid') as common_mid,
                      get_json_object(log, '$.common.ba') as common_ba,
                      get_json_object(log, '$.common.md') as common_md,
                      get_json_object(log, '$.common.os') as common_os,
                      get_json_object(log, '$.ts') as ts,  -- 解析时间戳
                      get_json_object(log, '$.common.sid') as common_sid,  -- 解析 session_id
                      get_json_object(log, '$.page') as page  -- 解析 page 字段（过滤用）
                  from ods_z_log
                  where dt = '20250630'
                    -- 过滤条件：page 非空 + 用户 ID 非空
                    and get_json_object(log, '$.page') is not null
                    and get_json_object(log, '$.common.uid') is not null
              ) t1
     ) t2
where rn = 1;
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
    execute_hive_insert(target_date, 'dwd_user_login_inc')