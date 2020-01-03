package com.tlink.streaming.sql;

import com.tlink.conf.TlinkConfigConstants;
import com.tlink.conf.TlinkConfiguration;
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

        TlinkConfiguration tConfig = new TlinkConfiguration(args[0].trim());

        String[] sourceFieldNames = PropertiesUtil.getStringArray(tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDNAMES, TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDNAMES_DEFAULT);
        TypeInformation<?>[] fieldTypes = tConfig.getSourceFieldTypes();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(tConfig.getTimeCharacteristic());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(PropertiesUtil.getInt(tConfig.getProperties(), TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PARALLELISM, TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PARALLELISM_DEFAULT));

        DataStream<Row> ds = env.addSource(new MemoryDataSource(tConfig)).
                returns(new RowTypeInfo(fieldTypes, sourceFieldNames));

        if (tConfig.isEventTimeTimeCharacteristic()){
            ds = ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator(tConfig));
        }

        String sourceTableName = tConfig.getProperties().getProperty(TlinkConfigConstants.TLINK_SOURCE_TABLE_NAME, TlinkConfigConstants.TLINK_SOURCE_TABLE_NAME_DEFAULT);

        tableEnv.registerDataStream(sourceTableName, ds, StringUtils.join(sourceFieldNames, ","));

        String sql = tConfig.getProperties().getProperty(TlinkConfigConstants.TLINK_STREAMING_SQL_STATEMENT);

        Table result = tableEnv.sqlQuery(sql);


        String sinkTableName = tConfig.getProperties().getProperty(TlinkConfigConstants.TLINK_SINK_TABLE_NAME, TlinkConfigConstants.TLINK_SINK_TABLE_NAME_DEFAULT);

        TableSink sink = StreamTableSinkFactory.getStreamTableSink(tConfig);
        tableEnv.registerTableSink(sinkTableName, sink);
        result.insertInto(sinkTableName);

        env.execute();
    }
}
