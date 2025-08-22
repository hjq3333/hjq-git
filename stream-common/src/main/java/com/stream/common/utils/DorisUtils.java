package com.stream.common.utils;

import com.stream.common.utils.ConfigUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;

import java.util.Properties;


public class DorisUtils {
    private static String DORIS_FE_NODES = null;
    private static String DORIS_DATABASE = null;

    private static void initializeConfig() {
        if (DORIS_FE_NODES == null || DORIS_DATABASE == null) {
            DORIS_FE_NODES = ConfigUtils.getString("doris.fe.nodes");
            DORIS_DATABASE = ConfigUtils.getString("doris.database");
        }
    }

    public static DorisSink<String> getDorisSink(String table) {
        initializeConfig();

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(DORIS_FE_NODES)
                        .setTableIdentifier(DORIS_DATABASE + "." + table)
                        .setUsername("root")
                        .setPassword("123456")
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
                .setSerializer(new SimpleStringSerializer())
                .build();

    }
}
