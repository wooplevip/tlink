package com.tlink.streaming.core;

import com.tlink.conf.TlinkConfigConstants;
import com.tlink.utils.PropertiesUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class MemoryAppendStreamTableSink implements AppendStreamTableSink<Row> {
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    private TlinkContext tConfig;

    public MemoryAppendStreamTableSink(TlinkContext tConfig) {
        this.tConfig = tConfig;
        this.fieldNames = PropertiesUtil.getStringArray(this.tConfig.getConfig(), TlinkConfigConstants.TLINK_SINK_TABLE_FIELDNAMES);
        this.fieldTypes = tConfig.getSinkFieldTypes();
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.addSink(new DataSink()).setParallelism(dataStream.getParallelism());
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(getFieldTypes(), getFieldNames());
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    private class DataSink extends RichSinkFunction<Row> {
        public DataSink() {
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            System.out.println("Result:" + value);
        }
    }
}
