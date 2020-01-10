package com.tlink.streaming.core;

import com.tlink.conf.SourceTable;
import com.tlink.conf.TlinkConfigConstants;
import com.tlink.utils.PropertiesUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MemoryDataSource extends RichParallelSourceFunction<Row> {
    private volatile boolean running = true;
    private TypeInformation<?>[] fieldTypes;
    private SourceTable sourceTable;

    public MemoryDataSource(SourceTable sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        long numElements = PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TOTAL, TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TOTAL_DEFAULT);
        int count = 0;
        String mode = sourceTable.getProperties().getProperty(TlinkConfigConstants.TLINK_SOURCE_PRODUCER_MODE, TlinkConfigConstants.TLINK_SOURCE_PRODUCER_MODE_DEFAULT);

        List<String> lines = new ArrayList<>(0);
        String path = sourceTable.getProperties().getProperty(TlinkConfigConstants.TLINK_SOURCE_PRODUCER_FILE_PATH);
        if (StringUtils.equalsIgnoreCase(mode, "file") && StringUtils.isNotEmpty(path)){
            lines = FileUtils.readLines(new File(path), "UTF-8");
            numElements = lines.size();
        }

        while (running && count < numElements) {
            if (StringUtils.isNotEmpty(sourceTable.getProperties().getProperty(TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_MS))) {
                Thread.sleep(PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_MS, 1000L));
            } else {
                Thread.sleep(RandomUtils.nextLong(
                        PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_STARTINCLUSIVE, 1),
                        PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_ENDEXCLUSIVE, 5)) *
                        PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_FACTOR, 1000L));
            }

            Row row = new Row(fieldTypes.length);

            if (StringUtils.equalsIgnoreCase(mode, "file")){
                produceRowFromFile(row, lines.get(count));
            }else {
                produceRandomRow(row);
            }

            ctx.collect(row);
            System.out.println("Sand data: " + row);
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

        this.fieldTypes = sourceTable.getFieldTypes();
    }

    private void produceRowFromFile(Row row, String line) throws Exception {
        String[] rows = line.split(",");
        for (int i = 0; i < rows.length; i++) {
            switch (fieldTypes[i].toString()) {
                case "Long":
                    row.setField(i, NumberUtils.toLong(rows[i].trim()));
                    break;
                case "Integer":
                    row.setField(i, NumberUtils.toInt(rows[i].trim()));
                    break;
                case "String":
                    row.setField(i, rows[i].trim());
                    break;
                default:
                    throw new Exception("Only support field types {LONG, INT, STRING, SQL_TIMESTAMP}");

            }
        }
    }

    private void produceRandomRow(Row row) throws Exception {
        int eventTimeIndex = PropertiesUtil.getInt(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_TABLE_EVENTTIME_INDEX, -1);

        for (int i = 0; i < fieldTypes.length; i++) {
            switch (fieldTypes[i].toString()) {
                case "Long": {
                    if (eventTimeIndex == i) {
                        long random = RandomUtils.nextLong(PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_STARTINCLUSIVE, 1),
                                PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_ENDEXCLUSIVE, 10)) *
                                PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_FACTOR, 1000L);
                        long rowTime = 0;

                        if (random % 2 == 0) {
                            rowTime = System.currentTimeMillis() - random;
                        } else {
                            rowTime = System.currentTimeMillis() + random;
                        }
                        row.setField(i, rowTime);
                    } else {
                        row.setField(i, RandomUtils.nextLong(PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_LONG_RANDOM_STARTINCLUSIVE, 1),
                                PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_LONG_RANDOM_ENDEXCLUSIVE, 10)) *
                                PropertiesUtil.getLong(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_LONG_RANDOM_FACTOR, 1));
                    }
                    break;
                }
                case "Integer":
                    row.setField(i, RandomUtils.nextInt(PropertiesUtil.getInt(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INT_RANDOM_STARTINCLUSIVE, 1),
                            PropertiesUtil.getInt(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INT_RANDOM_ENDEXCLUSIVE, 10)) *
                            PropertiesUtil.getInt(sourceTable.getProperties(), TlinkConfigConstants.TLINK_SOURCE_PRODUCER_INT_RANDOM_FACTOR, 1));
                    break;
                case "String": {
                    String[] values = PropertiesUtil.getStringArray(sourceTable.getProperties(),
                            TlinkConfigConstants.TLINK_SOURCE_PRODUCER_STRING_VALUES,
                            TlinkConfigConstants.TLINK_SOURCE_PRODUCER_STRING_VALUES_DEFAULT);

                    row.setField(i, values[RandomUtils.nextInt(0, values.length)]);
                    break;
                }
                default:
                    throw new Exception("Only support field types {LONG, INT, STRING, SQL_TIMESTAMP}");

            }
        }

    }
}
