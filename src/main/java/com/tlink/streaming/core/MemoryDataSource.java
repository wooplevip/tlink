package com.tlink.streaming.core;

import com.tlink.conf.TlinkConfigConstants;
import com.tlink.conf.TlinkConfiguration;
import com.tlink.utils.PropertiesUtil;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

public class MemoryDataSource extends RichParallelSourceFunction<Row> {
    private volatile boolean running = true;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;
    private TlinkConfiguration tConfig;


    public MemoryDataSource(TlinkConfiguration tConfig) {
        this.tConfig = tConfig;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        long numElements = PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TOTAL, TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TOTAL_DEFAULT);
        int count = 0;

        while (running && count < numElements) {
            if (StringUtils.isNotEmpty(this.tConfig.getProperties().getProperty(TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_MS))) {
                Thread.sleep(PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_MS, 1000L));
            } else {
                Thread.sleep(RandomUtils.nextLong(
                        PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_STARTINCLUSIVE, 1),
                        PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_ENDEXCLUSIVE, 5)) *
                        PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_FACTOR, 1000L));
            }

            Row row = new Row(fieldNames.length);
            int eventTimeIndex = PropertiesUtil.getInt(tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_TABLE_EVENTTIME_INDEX, -1);

            for (int i = 0; i < fieldNames.length; i++) {
                switch (fieldTypes[i].toString()) {
                    case "Long": {
                        if (eventTimeIndex == i) {
                            long random = RandomUtils.nextLong(PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_STARTINCLUSIVE, 1),
                                    PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_ENDEXCLUSIVE, 10)) *
                                    PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_FACTOR, 1000L);
                            long rowTime = 0;

                            if (random % 2 == 0) {
                                rowTime = System.currentTimeMillis() - random;
                            } else {
                                rowTime = System.currentTimeMillis() + random;
                            }
                            row.setField(i, rowTime);
                        } else {
                            row.setField(i, RandomUtils.nextLong(PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_LONG_RANDOM_STARTINCLUSIVE, 1),
                                    PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_LONG_RANDOM_ENDEXCLUSIVE, 10)) *
                                    PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_LONG_RANDOM_FACTOR, 1));
                        }
                        break;
                    }
                    case "Integer":
                        row.setField(i, RandomUtils.nextInt(PropertiesUtil.getInt(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INT_RANDOM_STARTINCLUSIVE, 1),
                                PropertiesUtil.getInt(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INT_RANDOM_ENDEXCLUSIVE, 10)) *
                                PropertiesUtil.getInt(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INT_RANDOM_FACTOR, 1));
                        break;
                    case "String": {
                        String[] values = PropertiesUtil.getStringArray(this.tConfig.getProperties(),
                                TlinkConfigConstants.TLINK_SOURCE_PRODUCER_STRING_VALUES,
                                TlinkConfigConstants.TLINK_SOURCE_PRODUCER_STRING_VALUES_DEFAULT);

                        row.setField(i, values[RandomUtils.nextInt(0, values.length)]);
                        break;
                    }
//                    case "Timestamp": {
//                        long random = RandomUtils.nextLong(PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_STARTINCLUSIVE, 1),
//                                PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_ENDEXCLUSIVE, 10)) *
//                                PropertiesUtil.getLong(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_FACTOR, 1000L);
//                        long rowTime = 0;
//
//                        if (random % 2 == 0) {
//                            rowTime = System.currentTimeMillis() - random;
//                        } else {
//                            rowTime = System.currentTimeMillis() + random;
//                        }
//                        row.setField(i, rowTime);
//                        break;
//                    }
                    default:
                        throw new Exception("Only support field types {LONG, INT, STRING, SQL_TIMESTAMP}");

                }
            }

            ctx.collect(row);
            System.out.println("sand data:" + row);
            count++;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.fieldNames = PropertiesUtil.getStringArray(this.tConfig.getProperties(), TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDNAMES, TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDNAMES_DEFAULT);
        this.fieldTypes = this.tConfig.getSourceFieldTypes();
    }
}
