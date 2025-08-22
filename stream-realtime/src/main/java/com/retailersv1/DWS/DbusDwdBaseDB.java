package com.retailersv1.DWS;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KwSplit;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DbusDwdBaseDB {

    private static final String PAGE_TOPIC = ConfigUtils.getString("kafka.page.topic");

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new MemoryStateBackend());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 读取页面日志 - 使用处理时间
        tEnv.executeSql("create table page_log(" +
                " page map<string, string>, " +
                " pt as PROCTIME() " +  // 处理时间字段
                ")" + SqlUtil.getKafka(PAGE_TOPIC,"first"));

        // 2. 读取搜索关键词
        Table kwTable = tEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "pt " +
                "from page_log " +
                "where  page['last_page_id'] ='search' " +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tEnv.createTemporaryView("kw_table", kwTable);

        // 3. 自定义分词函数
        tEnv.createTemporaryFunction("kw_split", KwSplit.class);

        Table keywordTable = tEnv.sqlQuery("select " +
                " keyword, " +
                " pt " +
                "from kw_table " +
                "join lateral table(kw_split(kw)) on true ");
        tEnv.createTemporaryView("keyword_table", keywordTable);

        // 4. 开窗聚合 - 使用处理时间窗口
        Table result = tEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, 'yyyyMMdd') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table keyword_table, descriptor(pt), interval '3' seconds ) ) " +
                "group by window_start, window_end, keyword ");

        result.execute().print();

//        // 5. 写出到 doris 中
//        tEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
//                "  stt string, " +  // 2023-07-11 14:14:14
//                "  edt string, " +
//                "  cur_date string, " +
//                "  keyword string, " +
//                "  keyword_count bigint " +
//                ")with(" +
//                " 'connector' = 'doris'," +
//                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
//                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
//                "  'username' = 'root'," +
//                "  'password' = '000000', " +
//                "  'sink.properties.format' = 'json', " +
//                "  'sink.buffer-count' = '4', " +
//                "  'sink.buffer-size' = '4086'," +
//                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
//                "  'sink.properties.read_json_by_line' = 'true' " +
//                ")");
//        result.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}
