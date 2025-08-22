package com.retailersv1.DWD;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;

import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DbusDwdTradeOrderRefund {

    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    private static final String DWD_TRADE_ORDER_REFUND = ConfigUtils.getString("kafka.dwd.trade.order.refund");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new MemoryStateBackend());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 读取统一源表（ODS层）
        tEnv.executeSql("CREATE TABLE ods_ecommerce_data (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time AS proctime(),\n" +
                "  et AS to_timestamp_ltz(ts_ms, 3),\n" +
                "  watermark for et as et - interval '3' second \n" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC,"first"));

        // 2. 读取HBase字典表
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SqlUtil.getHbaseDDL("dim_base_dic"));

        // 3. 过滤退单表数据（order_refund_info insert操作）
        Table orderRefundInfo = tEnv.sqlQuery("select " +
                "`after`['id'] id," +
                "`after`['user_id'] user_id," +
                "`after`['order_id'] order_id," +
                "`after`['sku_id'] sku_id," +
                "MD5(CAST(`after`['refund_type'] AS STRING)) refund_type," +
                "`after`['refund_num'] refund_num," +
                "`after`['refund_amount'] refund_amount," +
                "MD5(CAST(`after`['refund_reason_type'] AS STRING)) refund_reason_type," +
                "`after`['refund_reason_txt'] refund_reason_txt," +
                "`after`['create_time'] create_time," +
                "proc_time as pt," +
                "ts_ms  " +
                "from ods_ecommerce_data " +
                "where `source`['table']='order_refund_info' ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

//        orderRefundInfo.execute().print();


        // 4. 过滤订单表退单数据（order_info update操作）
        Table orderInfo = tEnv.sqlQuery("select " +
                "`after`['id'] id," +
                "`after`['province_id'] province_id " +
                "from ods_ecommerce_data " +
                "where `source`['table']='order_info' " +
                "and `op` = 'u' " +
                "and `after`['order_status']='1005'");
        tEnv.createTemporaryView("order_info", orderInfo);

//        orderInfo.execute().print();

        // 5. 多表关联
        Table result = tEnv.sqlQuery("select " +
                "ri.id," +
                "ri.user_id," +
                "ri.order_id," +
                "ri.sku_id," +
                "oi.province_id," +
                "date_format(ri.create_time, 'yyyy-MM-dd') date_id," +
                "ri.create_time," +
                "ri.refund_type refund_type_code," +
                "dic1.info.dic_name refund_type_name," +
                "ri.refund_reason_type refund_reason_type_code," +
                "dic2.info.dic_name refund_reason_type_name," +
                "ri.refund_reason_txt," +
                "ri.refund_num," +
                "ri.refund_amount," +
                "ri.ts_ms " +
                "from order_refund_info ri " +
                "join order_info oi " +
                "on ri.order_id = oi.id " +
                "join base_dic for system_time as of ri.pt as dic1 " +
                "on ri.refund_type = dic1.dic_code " +
                "join base_dic for system_time as of ri.pt as dic2 " +
                "on ri.refund_reason_type = dic2.dic_code ");

//        result.execute().print();

        // 6. 创建目标表并写出
        tEnv.executeSql("CREATE TABLE " + DWD_TRADE_ORDER_REFUND + " (\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "refund_type_code string,\n" +
                "refund_type_name string,\n" +
                "refund_reason_type_code string,\n" +
                "refund_reason_type_name string,\n" +
                "refund_reason_txt string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts_ms bigint,\n" +
                "primary key(id) not enforced\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_REFUND));

        result.executeInsert(DWD_TRADE_ORDER_REFUND);

    }
}
