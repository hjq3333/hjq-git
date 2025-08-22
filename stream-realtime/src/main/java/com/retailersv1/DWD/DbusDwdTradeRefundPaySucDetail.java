package com.retailersv1.DWD;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;

import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DbusDwdTradeRefundPaySucDetail {

    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");

    private static final String DWD_TRADE_REFUND_PAY_SUC_DETAIL = ConfigUtils.getString("kafka.dwd.trade.refund.pay.suc.detail");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new MemoryStateBackend());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 读取统一源表（替代readOdsDb方法）
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

        // 3. 过滤退款成功表数据（refund_payment update操作）
        Table refundPayment = tEnv.sqlQuery("select " +
                "`after`['id'] id," +
                "`after`['order_id'] order_id," +
                "`after`['sku_id'] sku_id," +
                "MD5(CAST(`after`['payment_type'] AS STRING)) payment_type," +
                "`after`['callback_time'] callback_time," +
                "`after`['total_amount'] total_amount," +
                "proc_time as pt," +
                "ts_ms  " +
                "from ods_ecommerce_data " +
                "where `source`['table']='refund_payment' " +
                "and `op`='u' " +
                "and `before`['refund_status'] is not null " +
                "and `after`['refund_status']='1602'");
        tEnv.createTemporaryView("refund_payment", refundPayment);



        // 4. 过滤退单表中的退单成功数据（order_refund_info update操作）
        Table orderRefundInfo = tEnv.sqlQuery("select " +
                "`after`['order_id'] order_id," +
                "`after`['sku_id'] sku_id," +
                "`after`['refund_num'] refund_num " +
                "from ods_ecommerce_data " +
                "where  `source`['table']='order_refund_info' " +
                "and `op`='u' " +
                "and `before`['refund_status'] is not null " +
                "and `after`['refund_status']='0705'");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);



        // 5. 过滤订单表中的退款成功数据（order_info update操作）
        Table orderInfo = tEnv.sqlQuery("select " +
                "`after`['id'] id," +
                "`after`['user_id'] user_id," +
                "`after`['province_id'] province_id " +
                "from ods_ecommerce_data " +
                "where `source`['table']='order_info' " +
                "and `op`='u' " +
                "and `before`['order_status'] is not null " +
                "and `after`['order_status']='1006'");
        tEnv.createTemporaryView("order_info", orderInfo);



        // 6. 四表关联（内连接 + 维表Lookup Join）
        Table result = tEnv.sqlQuery("select " +
                "rp.id," +
                "oi.user_id," +
                "rp.order_id," +
                "rp.sku_id," +
                "oi.province_id," +
                "rp.payment_type payment_type_code," +
                "dic.info.dic_name payment_type_name," +
                "date_format(rp.callback_time, 'yyyy-MM-dd') date_id," +
                "rp.callback_time," +
                "ori.refund_num," +
                "rp.total_amount refund_amount," +  // 重命名为refund_amount与目标表匹配
                "rp.ts_ms " +
                "from refund_payment rp " +
                "join order_refund_info ori " +
                "on rp.order_id = ori.order_id and rp.sku_id = ori.sku_id " +  // 双字段关联
                "join order_info oi " +
                "on rp.order_id = oi.id " +
                "join base_dic for system_time as of rp.pt as dic " +
                "on rp.payment_type = dic.dic_code ");

//        result.execute().print();

         //7. 创建目标表并写出数据
        tEnv.executeSql("CREATE TABLE " + DWD_TRADE_REFUND_PAY_SUC_DETAIL + " (\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "date_id string,\n" +
                "callback_time string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts_ms bigint,\n" +
                "primary key(id) not enforced\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_REFUND_PAY_SUC_DETAIL));

        result.executeInsert(DWD_TRADE_REFUND_PAY_SUC_DETAIL);

    }
}
