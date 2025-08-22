package com.retailersv1.DWD;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;

import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DbusDwdTradeOrderCancelDetail {

    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    private static final String DWD_TRADE_ORDER_CANCEL_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.cancel.detail");

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new MemoryStateBackend());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 创建统一的Kafka源表（ODS层）
        tEnv.executeSql("CREATE TABLE mysql_kafka (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time AS proctime()\n" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "order_cancel_processing"));

        // 2. 创建DWD层订单明细事实表的Kafka源表
        tEnv.executeSql("CREATE TABLE dwd_trade_order_detail (" +
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
                "ts_ms bigint " +
                ")" + SqlUtil.getKafka(DWD_TRADE_ORDER_DETAIL,"first"));

//        tEnv.executeSql("select * from dwd_trade_order_detail").print();

        // 3. 从统一源表过滤出订单取消数据（order_info表状态变更）
        Table orderCancel = tEnv.sqlQuery("select " +
                "`after`['id'] id, " +
                "`after`['operate_time'] operate_time, " +
                "ts_ms  " +
                "from mysql_kafka " +
                "where `source`['table']='order_info' " +
                "and `op` = 'u' " +
                "and `before`['order_status']='1001' " +
                "and `after`['order_status']='1003'");
        tEnv.createTemporaryView("order_cancel", orderCancel);

//        orderCancel.execute().print();



        // 4. 关联订单明细与取消信息
        Table result = tEnv.sqlQuery("select " +
                "od.id," +
                "od.order_id," +
                "od.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "date_format(oc.operate_time, 'yyyy-MM-dd') date_id," +
                "oc.operate_time cancel_time," +
                "od.sku_num," +
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +
                "oc.ts_ms " +
                "from dwd_trade_order_detail od " +
                "join order_cancel oc " +
                "on od.order_id=oc.id");

        //result.execute().print();



   // 5. 创建目标表并写入数据
        tEnv.executeSql("CREATE TABLE " + DWD_TRADE_ORDER_CANCEL_DETAIL + " (\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "date_id string,\n" +
                "cancel_time string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts_ms bigint,\n" +
                "primary key(id) not enforced\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_CANCEL_DETAIL));

        result.executeInsert(DWD_TRADE_ORDER_CANCEL_DETAIL);


    }
}
