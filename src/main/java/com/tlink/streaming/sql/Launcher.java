package com.tlink.streaming.sql;

import com.tlink.conf.TlinkConfigConstants;
import com.tlink.streaming.core.TlinkContext;
import com.tlink.utils.PropertiesUtil;
import com.tlink.utils.StreamTableSinkFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

public class Launcher {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Can not find config file path!");
            System.exit(101);
        }
        try {
            TlinkContext tContext = new TlinkContext(args[0].trim());

            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
            env.setStreamTimeCharacteristic(tContext.getTimeCharacteristic());

            EnvironmentSettings envSettings = tContext.getEnvironmentSettings();

            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
            env.setParallelism(PropertiesUtil.getInt(tContext.getConfig(), TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PARALLELISM, TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PARALLELISM_DEFAULT));

            tContext.registerTables(env, tableEnv);

            String sql = tContext.getConfig().getProperty(TlinkConfigConstants.TLINK_STREAMING_SQL_STATEMENT);

            Table result = tableEnv.sqlQuery(sql);

            String sinkTableName = tContext.getConfig().getProperty(TlinkConfigConstants.TLINK_SINK_TABLE_NAME, TlinkConfigConstants.TLINK_SINK_TABLE_NAME_DEFAULT);

            TableSink sink = StreamTableSinkFactory.getStreamTableSink(tContext);
            tableEnv.registerTableSink(sinkTableName, sink);
            result.insertInto(sinkTableName);

            if (PropertiesUtil.getBoolean(tContext.getConfig(), TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_EXPLAIN_ENABLED, false)){
                String explain = tableEnv.explain(result,true);
                System.out.println(explain);
            }

            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
