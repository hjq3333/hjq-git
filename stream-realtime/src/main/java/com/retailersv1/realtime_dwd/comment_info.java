package com.retailersv1.realtime_dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class comment_info {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        tEnv.executeSql("CREATE TABLE KafkaTable (" +
                "  `ts_ms` BIGINT," +
                "  `after` MAP<STRING,STRING>," +
                "  `source` MAP<STRING,STRING>," +
                "  `user_id` AS after['user_id']," +
                "  `nick_name` AS after['nick_name']," +
                "  `sku_id` AS after['sku_id']," +
                "  `spu_id` AS after['spu_id']," +
                "  `order_id` AS after['order_id']," +
                "  `appraise` AS after['appraise']," +
                "  `comment_txt` AS after['comment_txt']," +
                "  `create_time` AS after['create_time']" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'mysql_kafka'," +
                "  'properties.bootstrap.servers' = 'cdh01:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")");


        tEnv.executeSql("select user_id,nick_name,sku_id,spu_id" +
                " order_id,appraise,comment_txt,create_time from KafkaTable where source['table'] = 'comment_info' ");


        tEnv.executeSql("CREATE TABLE hTable (\n" +
                "         dic_code INT,\n" +
                "         info ROW<create_time  BIGINT, dic_code  STRING,dic_name STRING,operate_time STRING,parent_code STRING>,\n" +
                "         PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                "        ) WITH (\n" +
                "         'connector' = 'hbase-2.2',\n" +
                "         'table-name' = 'dim:dim_base_dic',\n" +
                "         'zookeeper.quorum' = 'cdh01:2181'\n" +
                "        );");


        tEnv.executeSql("select info.create_time,info.dic_code,\n" +
                "info.dic_name,info.operate_time,info.parent_code from hTable ");




    }
}
