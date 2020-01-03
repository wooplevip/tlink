package com.tlink.conf;

import com.google.common.collect.ImmutableMap;
import com.tlink.utils.PropertiesUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.*;

public class TlinkConfiguration implements Serializable {
    private String configPath;

    private Map<String, TypeInformation> supportedFieldTypes;
    private Map<String, TimeCharacteristic> supportedTimeCharacteristics;

    private Properties config = new Properties();


    public TlinkConfiguration(String configPath) {
        this.configPath = configPath;
        init();
    }

    private void init(){
        try {

            config.load(new FileInputStream(this.configPath));

            supportedFieldTypes = ImmutableMap.of("LONG", Types.LONG, "STRING", Types.STRING, "INT", Types.INT, "SQL_TIMESTAMP", Types.SQL_TIMESTAMP);
            supportedTimeCharacteristics = ImmutableMap.of("EVENT", TimeCharacteristic.EventTime, "PROCESSING", TimeCharacteristic.ProcessingTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public Properties getProperties() {
        return config;
    }

    public void validate() {

    }

    public Map<String, TypeInformation> getSupportedFieldTypes() {
        return supportedFieldTypes;
    }

    public TypeInformation<?>[] getSourceFieldTypes(){

        String[] types = PropertiesUtil.getStringArray(config, TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDTYPES, TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDTYPES_DEFAULT);
        TypeInformation<?>[] fieldTypes = new TypeInformation[types.length];

        for (int i = 0; i < fieldTypes.length; i++) {
            fieldTypes[i] = this.supportedFieldTypes.get(types[i]);
        }

        return fieldTypes;
    }

    public TypeInformation<?>[] getSinkFieldTypes(){
        String[] types = PropertiesUtil.getStringArray(config, TlinkConfigConstants.TLINK_SINK_TABLE_FIELDTYPES);
        TypeInformation<?>[] fieldTypes = new TypeInformation[types.length];

        for (int i = 0; i < fieldTypes.length; i++) {
            fieldTypes[i] = this.supportedFieldTypes.get(types[i]);
        }

        return fieldTypes;
    }



    public TimeCharacteristic getTimeCharacteristic(){
        String t = config.getProperty(TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_TIMECHARACTERISTIC);
        if (StringUtils.isEmpty(t)){
            return TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_TIMECHARACTERISTIC_DEFAULT;
        }else {
            return this.supportedTimeCharacteristics.get(t);
        }
    }

    public boolean isEventTimeTimeCharacteristic(){
        String t = config.getProperty(TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_TIMECHARACTERISTIC);
        if (StringUtils.isEmpty(t)){
            return false;
        }else {
            return this.supportedTimeCharacteristics.get(t) != TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_TIMECHARACTERISTIC_DEFAULT;
        }
    }
}
