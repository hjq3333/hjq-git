package com.retailersv1.DWD;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DbusDwdTradeOrderDetailAddKafka {

    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new MemoryStateBackend());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建统一的Kafka源表
        tEnv.executeSql("CREATE TABLE ods_ecommerce_data (\n" +
                "  `op` STRING,\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` MAP<STRING,STRING>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `ts_ms` BIGINT,\n" +
                "  proc_time AS proctime()\n" +
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "order_processing"));



        // 提取order_detail表数据
        Table orderDetail = tEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +
                "`after`['order_id'] order_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "`after`['sku_name'] sku_name,\n" +
                "`after`['create_time'] create_time,\n" +
                "`after`['source_id'] source_id,\n" +
                "`after`['source_type'] source_type,\n" +
                "`after`['sku_num'] sku_num,\n" +
                "cast(cast(`after`['sku_num'] as decimal(16,2)) * " +
                "cast(`after`['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                "`after`['split_total_amount'] split_total_amount,\n" +
                "`after`['split_activity_amount'] split_activity_amount,\n" +
                "`after`['split_coupon_amount'] split_coupon_amount,\n" +
                "ts_ms \n" +
                "from ods_ecommerce_data\n" +
                "where `source`['table'] = 'order_detail'");

//        orderDetail.execute().print();



        // 提取order_info表数据
        Table orderInfo = tEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['province_id'] province_id,\n" +
                "ts_ms \n" +
                "from ods_ecommerce_data\n" +
                "where `source`['table'] = 'order_info'");



        // 提取order_detail_activity表数据
        Table orderDetailActivity = tEnv.sqlQuery("select\n" +
                "`after`['order_detail_id'] order_detail_id,\n" +
                "`after`['activity_id'] activity_id,\n" +
                "`after`['activity_rule_id'] activity_rule_id,\n" +
                "ts_ms \n" +
                "from ods_ecommerce_data\n" +
                "where `source`['table'] = 'order_detail_activity'");



        // 提取order_detail_coupon表数据
        Table orderDetailCoupon = tEnv.sqlQuery("select\n" +
                "`after`['order_detail_id'] order_detail_id,\n" +
                "`after`['coupon_id'] coupon_id,\n" +
                "ts_ms \n" +
                "from ods_ecommerce_data\n" +
                "where `source`['table'] = 'order_detail_coupon'");



        // 将Table对象注册为临时视图
        tEnv.createTemporaryView("order_detail", orderDetail);
        tEnv.createTemporaryView("order_info", orderInfo);
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 四张表关联
        Table result = tEnv.sqlQuery("select\n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "oi.user_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "oi.province_id,\n" +
                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +
                "cou.coupon_id,\n" +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                "od.create_time,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "od.ts_ms\n" +
                "from order_detail od\n" +
                "join order_info oi on od.order_id=oi.id\n" +
                "left join order_detail_activity act on od.id=act.order_detail_id\n" +
                "left join order_detail_coupon cou on od.id=cou.order_detail_id");

//        result.execute().print();

    // 创建目标表并写入数据
        tEnv.executeSql("CREATE TABLE " + DWD_TRADE_ORDER_DETAIL + " (\n" +
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
                "create_time string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "ts_ms bigint,\n" +
                "primary key(id) not enforced\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_DETAIL));

        result.executeInsert(DWD_TRADE_ORDER_DETAIL);
    }
}
