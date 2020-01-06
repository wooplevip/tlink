package com.tlink.streaming.core;

import com.google.common.collect.ImmutableMap;
import com.tlink.conf.SourceTable;
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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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

    public String[] getSourceFieldNames(String tableName, boolean isTrimProctime){
        String key = StringUtils.isEmpty(tableName) ? TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDNAMES : "tlink.source.table." + tableName + ".fieldNames";
        String[] sourceFieldNames = PropertiesUtil.getStringArray(config, key, TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDNAMES_DEFAULT);

        String lastFieldName = sourceFieldNames[sourceFieldNames.length-1];
        if (isTrimProctime && StringUtils.endsWithIgnoreCase(lastFieldName, ".proctime")){
            return StringUtils.substringBeforeLast(StringUtils.join(sourceFieldNames, ","), ",").split(",");
        }else {
            return sourceFieldNames;
        }
    }

    public TypeInformation<?>[] getSourceFieldTypes(String tableName){
        String key = StringUtils.isEmpty(tableName) ? TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDTYPES : "tlink.source.table." + tableName + ".fieldTypes";
        String[] types = PropertiesUtil.getStringArray(config, key, TlinkConfigConstants.TLINK_SOURCE_TABLE_FIELDTYPES_DEFAULT);
        TypeInformation<?>[] fieldTypes = new TypeInformation[types.length];

        for (int i = 0; i < fieldTypes.length; i++) {
            fieldTypes[i] = this.supportedFieldTypes.get(types[i]);
        }

        return fieldTypes;
    }

    public TypeInformation<?>[] getSourceFieldTypes(){
        return getSourceFieldTypes("");
    }

    public String[] getSourceFieldNames(boolean isTrimProctime){
        return getSourceFieldNames("", isTrimProctime);
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

    public void registerTables(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv){
        String sourceTableName = this.getConfig().getProperty(TlinkConfigConstants.TLINK_SOURCE_TABLE_NAME, TlinkConfigConstants.TLINK_SOURCE_TABLE_NAME_DEFAULT);
        if (!this.isJoinSQL()){
            registerTable(sourceTableName, env, tableEnv);
        }else {
            String[] sourceTableNames = sourceTableName.split(",");
            for (String name : sourceTableNames) {
                registerTable(name, env, tableEnv);
            }
        }

    }

    private void registerTable(String sourceTableName, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv){
        SourceTable sourceTable = buildTable(sourceTableName);

        DataStream<Row> ds = env.addSource(new MemoryDataSource(sourceTable)).
                returns(new RowTypeInfo(sourceTable.getFieldTypes(), sourceTable.getFieldNames()));

        if (this.isEventTimeTimeCharacteristic()){
            ds = ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator(this));
        }

        tableEnv.registerDataStream(sourceTableName, ds, StringUtils.join(this.getSourceFieldNames(sourceTable.getName(),false), ","));
    }

    private SourceTable buildTable(String name){
        SourceTable sourceTable = new SourceTable();
        sourceTable.setName(name);
        sourceTable.setFieldNames(this.getSourceFieldNames(name,true));
        sourceTable.setFieldTypes(this.getSourceFieldTypes(name));

        sourceTable.setProperties(this.getConfig());
        return sourceTable;
    }
}
