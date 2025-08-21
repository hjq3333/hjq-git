package com.retailersv1;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DbusDwdTradeOrderPaySucDetail {

    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    private static final String DWD_TRADE_ORDER_PAY_SUC_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.pay.auc.detail");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new MemoryStateBackend());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 读取下单事务事实表
        tEnv.executeSql(
                "CREATE TABLE dwd_trade_order_detail (" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 3), " +
                        "watermark for et as et - interval '5' second " +
                        ")" + SqlUtil.getKafka(DWD_TRADE_ORDER_DETAIL,"first"));



        // 2. 读取统一源表（替代原readOdsDb方法）
        tEnv.executeSql("CREATE TABLE ods_ecommerce_data (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time AS proctime(),\n" +
                "  et AS to_timestamp_ltz(ts_ms, 3),\n" +
                "  watermark for et as et - interval '5' second \n" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "payment_info_consumer_group"));



        // 3. 读取HBase字典表（按指定格式）
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SqlUtil.getHbaseDDL("dim_base_dic"));


        // 4. 从统一源表过滤payment_info数据
        Table paymentInfo = tEnv.sqlQuery("select " +
                "`after`['user_id'] user_id," +
                "`after`['order_id'] order_id," +
                "MD5(CAST(`after`['payment_type'] AS STRING)) payment_type," +
                "`after`['callback_time'] callback_time," +
                "proc_time as pt," +
                "ts_ms as ts," +
                "et " +
                "from ods_ecommerce_data " +
                "where `source`['table']='payment_info' " +
                "and `after`['payment_status']='1602'");
        tEnv.createTemporaryView("payment_info", paymentInfo);



        // 5. 三表关联（interval join + 维表关联）
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.info.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");



        // 6. 写出到Kafka目标表
        tEnv.executeSql("CREATE TABLE " + DWD_TRADE_ORDER_PAY_SUC_DETAIL + " (" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint," +
                "primary key(order_detail_id) not enforced\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_PAY_SUC_DETAIL));

        result.executeInsert(DWD_TRADE_ORDER_PAY_SUC_DETAIL);

    }
}
