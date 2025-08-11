from pyspark.sql import SparkSession
import pymysql
import logging
from pyspark.sql import functions as F
from typing import Dict, List, Tuple

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../hive_mysql_sync.log'),
        logging.StreamHandler()
    ]
)

class HiveToMySQLSync:
    def __init__(self, spark: SparkSession, hive_db: str, mysql_config: Dict):
        self.spark = spark
        self.hive_db = hive_db
        self.mysql_config = mysql_config
        self.batch_size = 10000  # 批量插入记录数
        # 指定需要同步的表
        self.target_tables = [
            'ads_product_efficiency_monitor',
            'ads_product_range_analysis'
        ]

    def get_mysql_connection(self):
        """获取MySQL连接并设置自动重连"""
        return pymysql.connect(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database=self.mysql_config['database'],
            autocommit=True,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def get_hive_tables(self) -> List[str]:
        """获取指定需要同步的Hive表名"""
        # 只返回指定的表列表
        logging.info(f"准备同步 {len(self.target_tables)} 张表: {', '.join(self.target_tables)}")
        return self.target_tables

    def get_hive_table_schema(self, table_name: str) -> Tuple[List[Tuple], str]:
        """获取表结构和注释，增加列名去重逻辑"""
        # 获取列信息
        df = self.spark.sql(f"DESCRIBE {self.hive_db}.{table_name}")
        columns = []
        seen_columns = set()  # 用于跟踪已见过的列名

        for row in df.collect():
            if not row.col_name.startswith('#'):  # 跳过分区字段
                # 跳过重复的列
                if row.col_name in seen_columns:
                    logging.warning(f"表 {table_name} 中发现重复列: {row.col_name}，已跳过")
                    continue

                seen_columns.add(row.col_name)
                comment = row.comment if hasattr(row, 'comment') else ''
                columns.append((row.col_name, row.data_type.lower(), comment))

        # 获取表注释
        table_comment = self.spark.sql(
            f"SHOW TABLE EXTENDED IN {self.hive_db} LIKE '{table_name}'"
        ).collect()[0][0] or ""

        return columns, table_comment

    def convert_data_type(self, hive_type: str) -> str:
        """增强版类型映射，处理Hive与MySQL之间的类型不兼容问题"""
        type_map = {
            'string': 'VARCHAR(512)',
            'varchar': 'VARCHAR(255)',
            'char': 'CHAR(50)',
            'int': 'INT',
            'bigint': 'BIGINT',
            'double': 'DOUBLE',
            'float': 'FLOAT',
            'boolean': 'TINYINT(1)',
            'timestamp': 'TIMESTAMP',  # MySQL的TIMESTAMP类型
            'date': 'DATE',
            'binary': 'BLOB'
        }
        # 处理数组类型
        if hive_type.startswith('array<'):
            return 'TEXT'  # MySQL中使用TEXT存储数组
        if hive_type.startswith('decimal'):
            return hive_type.upper().replace('decimal', 'DECIMAL')
        return type_map.get(hive_type, 'TEXT')

    def generate_mysql_ddl(self, table_name: str, columns: List[Tuple], comment: str) -> str:
        """生成MySQL建表语句，包含主键检测"""
        column_defs = []
        primary_keys = []

        for col in columns:
            name, col_type, col_comment = col
            mysql_type = self.convert_data_type(col_type)
            comment_clause = f" COMMENT '{col_comment}'" if col_comment else ''

            # 假设名为id的字段是主键
            if name.lower() == 'id':
                primary_keys.append(f"`{name}`")

            column_defs.append(f" `{name}` {mysql_type}{comment_clause}")

        # 添加主键约束
        if primary_keys:
            column_defs.append(f" PRIMARY KEY ({', '.join(primary_keys)})")

        column_defs_str = ',\n '.join(column_defs)
        return f"""
        CREATE TABLE IF NOT EXISTS {self.mysql_config['database']}.{table_name} (
            {column_defs_str}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{comment}'
        """

    def sync_table_structure(self, table_name: str):
        """同步表结构到MySQL"""
        columns, comment = self.get_hive_table_schema(table_name)
        ddl = self.generate_mysql_ddl(table_name, columns, comment)

        try:
            with self.get_mysql_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {self.mysql_config['database']}.{table_name}")
                    cursor.execute(ddl)
            logging.info(f"表 {table_name} 结构同步成功")
        except Exception as e:
            logging.error(f"同步表 {table_name} 结构失败: {str(e)}")
            raise

    def sync_table_data(self, table_name: str):
        """批量同步数据到MySQL，处理数据类型转换问题"""
        try:
            # 为特定表添加类型转换以解决Parquet读取问题
            if table_name == 'ads_hot_product_ranking':
                df = self.spark.read.table(f"{self.hive_db}.{table_name}") \
                    .filter(F.col("stat_period") == "day") \
                    .select(
                    F.col("stat_date"),
                    F.col("product_id"),
                    F.col("product_name"),
                    F.col("category_id"),
                    F.col("total_sales_amount"),
                    F.col("total_visit_num").cast("bigint").alias("total_visit_num"),
                    F.col("conversion_rate"),
                    F.col("hot_rank"),
                    F.col("stat_period")
                )
            elif table_name == 'ads_platform_core_index':
                df = self.spark.read.table(f"{self.hive_db}.{table_name}") \
                    .filter(F.col("stat_period") == "day") \
                    .select(
                    F.col("stat_date"),
                    F.col("total_visitor_num"),
                    F.col("total_visit_num").cast("bigint").alias("total_visit_num"),
                    F.col("total_order_num").cast("bigint").alias("total_order_num"),
                    F.col("total_sales_amount"),
                    F.col("avg_order_amount"),
                    F.col("pay_conversion_rate"),
                    F.col("visitor_avg_stay_time"),
                    F.col("yoy_growth_rate"),
                    F.col("mom_growth_rate"),
                    F.col("stat_period")
                )
            elif table_name == 'ads_category_sale_ranking':
                df = self.spark.read.table(f"{self.hive_db}.{table_name}") \
                    .filter(F.col("stat_period") == "day") \
                    .select(
                    F.col("stat_date"),
                    F.col("category_id"),
                    F.col("category_name"),
                    F.col("total_sales_amount"),
                    F.col("total_sales_num").cast("bigint").alias("total_sales_num"),
                    F.col("sales_contribution"),
                    F.col("sales_rank"),
                    F.col("stat_period")
                )
            elif table_name == 'ads_hot_search_word':
                # 特别处理包含数组的表
                df = self.spark.read.table(f"{self.hive_db}.{table_name}") \
                    .filter(F.col("stat_period") == "day") \
                    .select(
                    F.col("stat_date"),
                    F.col("search_word"),
                    F.col("total_search_num").cast("int").alias("total_search_num"),
                    F.col("click_rate"),
                    F.col("search_rank"),
                    F.col("related_hot_product_ids"),  # 保持原样，后续处理
                    F.col("stat_period")
                )
            else:
                # 对于其他表，直接读取
                df = self.spark.read.table(f"{self.hive_db}.{table_name}")

            # 对于包含数组的列，需要特殊处理
            if table_name == 'ads_hot_search_word':
                # 将数组列转换为字符串
                df = df.withColumn("related_hot_product_ids",
                                   F.when(F.col("related_hot_product_ids").isNotNull(),
                                          F.concat(F.lit("["),
                                                   F.array_join(F.col("related_hot_product_ids"), ","),
                                                   F.lit("]")))
                                   .otherwise(F.lit("[]")))

            columns = df.schema.names
            rows = df.collect()

            with self.get_mysql_connection() as conn:
                with conn.cursor() as cursor:
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

                    for row in rows:
                        cursor.execute(insert_sql, tuple(row))

            logging.info(f"表 {table_name} 数据同步完成，记录数: {len(rows)}")
        except Exception as e:
            logging.error(f"同步表 {table_name} 数据失败: {str(e)}")
            raise

    def run_sync(self):
        """执行完整同步流程"""
        tables = self.get_hive_tables()
        for table in tables:
            try:
                self.sync_table_structure(table)
                self.sync_table_data(table)
            except Exception as e:
                logging.error(f"表 {table} 同步失败: {str(e)}，跳过继续处理其他表")
                continue

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EnhancedHiveMySQLSync") \
        .config("spark.local.dir", "D:/spark_temp") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .enableHiveSupport() \
        .getOrCreate()

    config = {
        'host': '192.168.142.130',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'gd01'
    }

    syncer = HiveToMySQLSync(spark, "gd01", config)
    syncer.run_sync()
    spark.stop()
