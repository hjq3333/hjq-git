package com.stream.common.utils;

import com.stream.common.utils.ConfigUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;


import java.util.Properties;

public class DorisUtils {
    private static final String DORIS_FE_NODES = ConfigUtils.getString("doris.fe.nodes");
    private static final String DORIS_DATABASE = ConfigUtils.getString("doris.database");

    public static DorisSink<String> getDorisSink(String table) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        // 允许更高比例的数据被过滤
        props.setProperty("max_filter_ratio", "0.8");
        // 或临时关闭严格模式
        props.setProperty("strict_mode", "false");



        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(DORIS_FE_NODES)
                        .setTableIdentifier(DORIS_DATABASE + "." + table)
                        .setUsername("admin")
                        .setPassword("")
                        .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .disable2PC()
                        .setBufferCount(3)
                        .setBufferSize(1024 * 1024)
                        .setCheckInterval(3000)
                        .setMaxRetries(3)
                        .setStreamLoadProp(props)
                        .build())
                .setSerializer(new SimpleStringSerializer()) // 修复点：显式实例化
                .build();
        return sink;
    }
}
