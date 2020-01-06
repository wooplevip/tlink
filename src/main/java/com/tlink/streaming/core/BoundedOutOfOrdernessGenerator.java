package com.tlink.streaming.core;

import com.tlink.conf.TlinkConfigConstants;
import com.tlink.utils.PropertiesUtil;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Row> {
    private long maxOutOfOrderness;
    private int eventTimeIndex;

    private long currentMaxTimestamp;

    public BoundedOutOfOrdernessGenerator(TlinkContext tConfig) {
        maxOutOfOrderness = PropertiesUtil.getLong(tConfig.getConfig(), TlinkConfigConstants.TLINK_SOURCE_WATERMARK_MAXOUTOFORDERNESS, 10000L);
        eventTimeIndex = PropertiesUtil.getInt(tConfig.getConfig(), TlinkConfigConstants.TLINK_SOURCE_TABLE_EVENTTIME_INDEX, 0);
    }

    @Override
    public long extractTimestamp(Row value, long previousElementTimestamp) {
        //System.out.println("value is " + value);
        long timestamp = NumberUtils.toLong(String.valueOf(value.getField(eventTimeIndex)));
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        //System.out.println("watermark:" + StringUtilsPlus.stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}