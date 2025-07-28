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


def select_to_hive(jdbcDF, tableName, partition_date):
    # 使用insertInto方法写入已存在的分区表
    jdbcDF.write \
        .mode('append') \
        .insertInto(f"dim.{tableName}")



# 2. 执行Hive SQL插入操作
def execute_hive_insert(partition_date: str, tableName):
    spark = get_spark_session()

    # 构建SQL语句，修正字段别名以匹配Hive表结构
    select_sql = f"""
WITH 
    sku AS (
        SELECT
            id,
            price,
            sku_name,
            sku_desc,
            weight,
            is_sale,
            spu_id,
            category3_id,
            tm_id,
            create_time
        FROM ods_sku_info
        WHERE `dt`='20250630'
    ),
    spu AS (
        SELECT
            id,
            spu_name
        FROM ods_spu_info
        WHERE `dt`='20250630'
    ),
    c3 AS (
        SELECT
            id,
            name,
            category2_id
        FROM ods_base_category3
        WHERE `dt`='20250630'
    ),
    c2 AS (
        SELECT
            id,
            name,
            category1_id
        FROM ods_base_category2
        WHERE `dt`='20250630'
    ),
    c1 AS (
        SELECT
            id,
            name
        FROM ods_base_category1
        WHERE `dt`='20250630'
    ),
    tm AS (
        SELECT
            id,
            tm_name
        FROM ods_base_trademark
        WHERE `dt`='20250630'
    ),
    attr AS (
        SELECT
            sku_id,
            collect_set(
                    named_struct(
                            'attr_id', CAST(attr_id AS STRING),
                            'value_id', CAST(value_id AS STRING),
                            'attr_name', attr_name,
                            'value_name', value_name
                        )
                ) as attrs
        FROM ods_sku_attr_value
        WHERE `dt`='20250630'
        GROUP BY sku_id
    ),
    sale_attr AS (
        SELECT
            sku_id,
            collect_set(
                    named_struct(
                            'sale_attr_id', CAST(sale_attr_id AS STRING),
                            'sale_attr_value_id', CAST(sale_attr_value_id AS STRING),
                            'sale_attr_name', sale_attr_name,
                            'sale_attr_value_name', sale_attr_value_name
                        )
                ) as sale_attrs
        FROM ods_sku_sale_attr_value
        WHERE `dt`='20250630'
        GROUP BY sku_id
    )
SELECT
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    CAST(sku.is_sale AS BOOLEAN) AS is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name AS category3_name,
    c3.category2_id,
    c2.name AS category2_name,
    c2.category1_id,
    c1.name AS category1_name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
FROM sku
LEFT JOIN spu ON sku.spu_id = spu.id
LEFT JOIN c3 ON sku.category3_id = c3.id
LEFT JOIN c2 ON c3.category2_id = c2.id
LEFT JOIN c1 ON c2.category1_id = c1.id
LEFT JOIN tm ON sku.tm_id = tm.id
LEFT JOIN attr ON sku.id = attr.sku_id
LEFT JOIN sale_attr ON sku.id = sale_attr.sku_id;
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
    target_date = '2025-07-10'

    # 执行插入操作
    execute_hive_insert(target_date, 'dim_sku_full')