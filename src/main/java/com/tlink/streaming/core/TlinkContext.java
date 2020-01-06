package com.tlink.streaming.core;

import com.google.common.collect.ImmutableMap;
import com.tlink.conf.TlinkConfigConstants;
import com.tlink.utils.PropertiesUtil;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.table.api.EnvironmentSettings;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.*;

public class TlinkContext implements Serializable {
    private String configPath;

    private Map<String, TypeInformation> supportedFieldTypes;
    private Map<String, TimeCharacteristic> supportedTimeCharacteristics;

    private boolean isJoin = false;
    private Properties config = new Properties();


    public TlinkContext(String configPath) {
        this.configPath = configPath;
        init();
    }

    private void init(){
        try {

            config.load(new FileInputStream(this.configPath));

            supportedFieldTypes = ImmutableMap.of("LONG", Types.LONG, "STRING", Types.STRING, "INT", Types.INT, "SQL_TIMESTAMP", Types.SQL_TIMESTAMP);
            supportedTimeCharacteristics = ImmutableMap.of("EVENT", TimeCharacteristic.EventTime, "PROCESSING", TimeCharacteristic.ProcessingTime);

            validate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Properties getConfig() {
        return config;
    }

    private void validate() throws Exception {
        String sql = config.getProperty(TlinkConfigConstants.TLINK_STREAMING_SQL_STATEMENT);
        SqlParser.Config sqlConfig = SqlParser.configBuilder()
                .setCaseSensitive(false).setLex(Lex.MYSQL).build();
        SqlParser parser = SqlParser.create(sql, sqlConfig);
        SqlNode sqlNode = parser.parseQuery();
        parserSql(sqlNode);
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

    public String[] getSourceFieldNames(boolean isTrimProctime){
        String[] sourceFieldNames = PropertiesUtil.getStringArray(config, TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDNAMES, TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDNAMES_DEFAULT);

        String lastFieldName = sourceFieldNames[sourceFieldNames.length-1];
        if (isTrimProctime && StringUtils.endsWithIgnoreCase(lastFieldName, ".proctime")){
            return StringUtils.substringBeforeLast(StringUtils.join(sourceFieldNames, ","), ",").split(",");
        }else {
            return sourceFieldNames;
        }
    }

    public EnvironmentSettings getEnvironmentSettings(){
        EnvironmentSettings.Builder envSettings = EnvironmentSettings.newInstance().inStreamingMode();
        if (config.getProperty(TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PLANNER, TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PLANNER_DEFAULT)
        .equalsIgnoreCase(TlinkConfigConstants.TLINK_STREAMING_SQL_ENV_PLANNER_DEFAULT)){

            return envSettings.useOldPlanner().build();
        }else {
            return envSettings.useBlinkPlanner().build();
        }
    }

    public boolean isJoinSQL() {
        return isJoin;
    }

    private void parserSql(SqlNode sqlNode){
        SqlKind sqlKind = sqlNode.getKind();

        switch (sqlKind){
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
                parserSql(sqlFrom);
                break;
            case JOIN:
                isJoin = true;
                break;
            default:
                break;
        }
    }

    public void registerTables(){

    }
}
