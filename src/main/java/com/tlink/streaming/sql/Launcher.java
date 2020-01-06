package com.tlink.streaming.sql;

import com.tlink.conf.TlinkConfigConstants;
import com.tlink.streaming.core.TlinkContext;
import com.tlink.streaming.core.BoundedOutOfOrdernessGenerator;
import com.tlink.streaming.core.MemoryDataSource;
import com.tlink.utils.PropertiesUtil;
import com.tlink.utils.StreamTableSinkFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class Launcher {
    public static void main(String[] args) throws Exception{
        if (args.length == 0){
            System.err.println("Can not find config file path!");
            System.exit(101);
        }

        TlinkContext tContext = new TlinkContext(args[0].trim());

        TypeInformation<?>[] fieldTypes = tContext.getSourceFieldTypes();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(tContext.getTimeCharacteristic());

        EnvironmentSettings envSettings = tContext.getEnvironmentSettings();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
        env.setParallelism(PropertiesUtil.getInt(tContext.getConfig(), TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PARALLELISM, TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PARALLELISM_DEFAULT));

        DataStream<Row> ds = env.addSource(new MemoryDataSource(tContext)).
                returns(new RowTypeInfo(fieldTypes, tContext.getSourceFieldNames(true)));

        if (tContext.isEventTimeTimeCharacteristic()){
            ds = ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator(tContext));
        }

        String sourceTableName = tContext.getConfig().getProperty(TlinkConfigConstants.TLINK_SOURCE_TABLE_NAME, TlinkConfigConstants.TLINK_SOURCE_TABLE_NAME_DEFAULT);

        tableEnv.registerDataStream(sourceTableName, ds, StringUtils.join(tContext.getSourceFieldNames(false), ","));

        String sql = tContext.getConfig().getProperty(TlinkConfigConstants.TLINK_STREAMING_SQL_STATEMENT);

        Table result = tableEnv.sqlQuery(sql);

        String sinkTableName = tContext.getConfig().getProperty(TlinkConfigConstants.TLINK_SINK_TABLE_NAME, TlinkConfigConstants.TLINK_SINK_TABLE_NAME_DEFAULT);

        TableSink sink = StreamTableSinkFactory.getStreamTableSink(tContext);
        tableEnv.registerTableSink(sinkTableName, sink);
        result.insertInto(sinkTableName);

        env.execute();
    }
}
