package com.retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.MapUpdateHbaseDimTableFunc;
import com.retailersv1.func.ProcessSpiltStreamToHBaseDimFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlCDCToKafkaHbase {

    private static final String zookeeper_server = ConfigUtils.getString("zookeeper.server.host.list");

    private static final String kafka_server = ConfigUtils.getString("kafka.bootstrap.servers");

    private static final String hbase_name = ConfigUtils.getString("hbase.namespace");

    private static final String mysql_to_kafka_topic = ConfigUtils.getString("kafka.cdc.db.topic");

    @SneakyThrows
    public static void main(String[] args) throws Exception {

        //设置HADOOP用户
        System.setProperty("HADOOP_USER_NAME","root");
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置flink默认参数
        EnvironmentSettingUtils.defaultParameter(env);

        //主数据库CDC源（监听业务数据变更）
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        //配置库CDC源（监听维度表配置变更）
        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"), // 配置库名称
                "realtime_v1_config.table_process_dim", // 具体监听的配置表
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        // 将CDC源转换为Flink数据流
        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");

        //字符串解析为JSONObject
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);

        //发送到Kafka（将JSONObject转回字符串）
        cdcDbMainStreamMap.map(JSONObject::toString)
                .sinkTo(
                        KafkaUtils.buildKafkaSink(kafka_server, mysql_to_kafka_topic)
                )
                .uid("mysql_cdc_to_kafka_topic")
                .name("mysql_cdc_to_kafka_topic");

        cdcDbMainStreamMap.print("cdcDbMainStreamMap -> ");


        //字符串解析为JSONObject
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMap = cdcDbDimStream.map(JSONObject::parseObject)
                .uid("dim_data_convert_json")
                .name("dim_data_convert_json")
                .setParallelism(1);

        //清洗数据：移除无用字段（source/transaction），保留操作类型（op）和数据（before/after）
        SingleOutputStreamOperator<JSONObject> cdcDbDimStreamMapCleanColumn = cdcDbDimStreamMap.map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))) {
                        resJson.put("before", s.getJSONObject("before"));
                    } else {
                        resJson.put("after", s.getJSONObject("after"));
                    }
                    resJson.put("op", s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");

        //生成HBase维度表配置
        SingleOutputStreamOperator<JSONObject> tpDS = cdcDbDimStreamMapCleanColumn.map(new MapUpdateHbaseDimTableFunc(zookeeper_server, hbase_name))
                .uid("map_create_hbase_dim_table")
                .name("map_create_hbase_dim_table");

        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapstageDesc", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcastDS = tpDS.broadcast(mapStageDesc);
        BroadcastConnectedStream<JSONObject, JSONObject> connectDS = cdcDbMainStreamMap.connect(broadcastDS);

        connectDS.process(new ProcessSpiltStreamToHBaseDimFunc(mapStageDesc));

        env.disableOperatorChaining();
        env.execute();
    }
}
