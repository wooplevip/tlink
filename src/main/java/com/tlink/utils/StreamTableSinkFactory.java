package com.tlink.utils;

import com.tlink.conf.TlinkConfigConstants;
import com.tlink.conf.TlinkConfiguration;
import com.tlink.streaming.core.MemoryAppendStreamTableSink;
import com.tlink.streaming.core.MemoryRetractStreamTableSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.sinks.TableSink;

public class StreamTableSinkFactory {
    public static TableSink getStreamTableSink(TlinkConfiguration tConfig) {
        String sinkTableType = tConfig.getProperties().getProperty(TlinkConfigConstants.TLINK_SINK_TABLE_TYPE, "Append").trim();
        if (StringUtils.equalsIgnoreCase(sinkTableType, "Append")){
            return new MemoryAppendStreamTableSink(tConfig);
        }else if (StringUtils.equalsIgnoreCase(sinkTableType, "Retract")){
            return new MemoryRetractStreamTableSink(tConfig);
        }

        return null;
    }
}
