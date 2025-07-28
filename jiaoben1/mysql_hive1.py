from pyspark.sql import SparkSession
import pymysql
import os


def get_mysql_connection(config):
    return pymysql.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )


def ensure_hdfs_path(spark, path):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hdfs_path):
        fs.mkdirs(hdfs_path)


def get_mysql_tables(conn, db_name):
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW TABLES FROM {db_name}")
        return [table[0] for table in cursor.fetchall()]


def get_table_comment(conn, db_name, table_name):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT TABLE_COMMENT 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA='{db_name}' 
            AND TABLE_NAME='{table_name}'
        """)
        return cursor.fetchone()[0] or ""


def get_table_columns(conn, db_name, table_name):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT COLUMN_NAME,
                   CASE
                       WHEN DATA_TYPE IN ('varchar','char','text') THEN 'STRING'
                       WHEN DATA_TYPE IN ('int','tinyint') THEN 'INT'
                       WHEN DATA_TYPE = 'bigint' THEN 'BIGINT'
                       WHEN DATA_TYPE = 'decimal' THEN CONCAT('DECIMAL(',NUMERIC_PRECISION,',',NUMERIC_SCALE,')')
                       ELSE 'STRING'
                   END AS hive_type,
                   COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        return cursor.fetchall()


def generate_create_ddl(hive_db, table_name, columns, table_comment):
    column_defs = []
    for col in columns:
        comment = f" COMMENT '{col[2]}'" if col[2] else ''
        column_defs.append(f"    `{col[0]}` {col[1]}{comment}")
    formatted_columns = ',\n'.join(column_defs)
    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {hive_db}.ods_{table_name} (
{formatted_columns}
) COMMENT '{table_comment}'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
LOCATION '/warehouse/{hive_db}/ods/ods_{table_name}/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')
"""


def sync_schema(mysql_conf, spark):
    ddl_statements = []
    try:
        mysql_conn = get_mysql_connection(mysql_conf)
        tables = get_mysql_tables(mysql_conn, mysql_conf['database'])
        print(f"开始同步 {len(tables)} 张表结构")

        for table in tables:
            try:
                columns = get_table_columns(mysql_conn, mysql_conf['database'], table)
                table_comment = get_table_comment(mysql_conn, mysql_conf['database'], table)

                hdfs_path = f"/warehouse/{mysql_conf['database']}/ods/ods_{table}"
                ensure_hdfs_path(spark, hdfs_path)

                spark.sql(f"DROP TABLE IF EXISTS {mysql_conf['database']}.ods_{table}")
                create_ddl = generate_create_ddl(mysql_conf['database'], table, columns, table_comment)
                ddl_statements.append(create_ddl)
                spark.sql(create_ddl)
                print(f"表 {table} 同步完成")
            except Exception as e:
                print(f"处理表 {table} 时出错: {str(e)}")
                continue

        # 输出所有DDL语句到文件
        output_file = f"{mysql_conf['database']}_hive_ddl.sql"
        with open(output_file, 'w') as f:
            f.write("\n\n".join(ddl_statements))
        print(f"所有DDL语句已保存到: {os.path.abspath(output_file)}")

    finally:
        if 'mysql_conn' in locals() and mysql_conn:
            mysql_conn.close()
    return ddl_statements


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MySQL2HiveSync") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
        .enableHiveSupport() \
        .getOrCreate()

    mysql_config = {
        'host': '192.168.142.130',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'dmp_ad'
    }

    try:
        ddl_list = sync_schema(mysql_config, spark)
        print("\n生成的DDL语句如下:")
        for idx, ddl in enumerate(ddl_list, 1):
            print(f"\n-- 表 {idx} --")
            print(ddl)
    finally:
        spark.stop()

