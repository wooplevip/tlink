package com.tlink.conf;

import org.apache.flink.streaming.api.TimeCharacteristic;

public class TlinkConfigConstants {
    public static final String TLINK_SOURCE_TABLE_NAMES = "tlink.source.table.names";
    public static final String TLINK_SOURCE_TABLE_MAX = "tlink.source.table.max";
    public static final int TLINK_SOURCE_TABLE_MAX_DEFAULT = 2;
    public static final String TLINK_SOURCE_TABLE_NAME_DEFAULT = "Orders";
    public static final String TLINK_SOURCE_TABLE_FIELDNAMES = "tlink.source.table.fieldNames";
    public static final String[] TLINK_SOURCE_TABLE_FIELDNAMES_DEFAULT = {"user", "product", "amount"};
    public static final String TLINK_SOURCE_TABLE_FIELDTYPES = "tlink.source.table.fieldTypes";
    public static final String[] TLINK_SOURCE_TABLE_FIELDTYPES_DEFAULT = {"LONG", "STRING", "INT"};

    public static final String TLINK_SOURCE_TABLE_EVENTTIME_INDEX = "tlink.source.eventTime.index";
    public static final String TLINK_SOURCE_WATERMARK_MAXOUTOFORDERNESS = "tlink.source.watermark.maxOutOfOrderness";

    public static final String TLINK_SOURCE_PRODUCER_MODE = "tlink.source.producer.mode";//file or random
    public static final String TLINK_SOURCE_PRODUCER_MODE_DEFAULT = "random";

    public static final String TLINK_SOURCE_PRODUCER_FILE_PATH = "tlink.source.producer.file.path";

    public static final String TLINK_SOURCE_PRODUCER_TOTAL = "tlink.source.producer.total";
    public static final long TLINK_SOURCE_PRODUCER_TOTAL_DEFAULT = 20L;

    public static final String TLINK_SOURCE_PRODUCER_INTERVAL_MS = "tlink.source.producer.interval.ms";
    public static final String TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_STARTINCLUSIVE = "tlink.source.producer.interval.random.startInclusive";
    public static final String TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_ENDEXCLUSIVE = "tlink.source.producer.interval.random.endExclusive";
    public static final String TLINK_SOURCE_PRODUCER_INTERVAL_RANDOM_FACTOR = "tlink.source.producer.interval.random.factor";

    public static final String TLINK_SOURCE_PRODUCER_STRING_VALUES = "tlink.source.producer.string.values";
    public static final String[] TLINK_SOURCE_PRODUCER_STRING_VALUES_DEFAULT = {"foo", "bar", "baz"};

    public static final String TLINK_SOURCE_PRODUCER_LONG_RANDOM_STARTINCLUSIVE = "tlink.source.producer.long.random.startInclusive";
    public static final String TLINK_SOURCE_PRODUCER_LONG_RANDOM_ENDEXCLUSIVE = "tlink.source.producer.long.random.endExclusive";
    public static final String TLINK_SOURCE_PRODUCER_LONG_RANDOM_FACTOR = "tlink.source.producer.long.random.factor";

    public static final String TLINK_SOURCE_PRODUCER_INT_RANDOM_STARTINCLUSIVE = "tlink.source.producer.int.random.startInclusive";
    public static final String TLINK_SOURCE_PRODUCER_INT_RANDOM_ENDEXCLUSIVE = "tlink.source.producer.int.random.endExclusive";
    public static final String TLINK_SOURCE_PRODUCER_INT_RANDOM_FACTOR = "tlink.source.producer.int.random.factor";

    public static final String TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_STARTINCLUSIVE = "tlink.source.producer.timestamp.random.startInclusive";
    public static final String TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_ENDEXCLUSIVE = "tlink.source.producer.timestamp.random.endExclusive";
    public static final String TLINK_SOURCE_PRODUCER_TIMESTAMP_RANDOM_FACTOR = "tlink.source.producer.timestamp.random.factor";


    public static final String TLINK_SINK_TABLE_NAME = "tlink.sink.table.name";
    public static final String TLINK_SINK_TABLE_NAME_DEFAULT = "Output";
    public static final String TLINK_SINK_TABLE_FIELDNAMES = "tlink.sink.table.fieldNames";
    public static final String TLINK_SINK_TABLE_FIELDTYPES = "tlink.sink.table.fieldTypes";
    public static final String TLINK_SINK_TABLE_TYPE = "tlink.sink.table.type";//Append or Retract


    public static final String TLINK_STREAMING_SQL_ENV_PARALLELISM = "tlink.streaming.sql.env.parallelism";
    public static final int TLINK_STREAMING_SQL_ENV_PARALLELISM_DEFAULT = 1;

    public static final String TLINK_STREAMING_SQL_ENV_TIMECHARACTERISTIC = "tlink.streaming.sql.env.timeCharacteristic";
    public static final TimeCharacteristic TLINK_STREAMING_SQL_ENV_TIMECHARACTERISTIC_DEFAULT = TimeCharacteristic.ProcessingTime;
    public static final String TLINK_STREAMING_SQL_ENV_PLANNER = "tlink.streaming.sql.env.planner";
    public static final String TLINK_STREAMING_SQL_ENV_PLANNER_DEFAULT = "old"; //old or blink
    public static final String TLINK_STREAMING_SQL_ENV_EXPLAIN_ENABLED = "tlink.streaming.sql.env.explain.enabled";

    public static final String TLINK_STREAMING_SQL_STATEMENT = "tlink.streaming.sql.statement";
}
