package com.tlink.conf;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.Properties;

public class SourceTable implements Serializable {
    private String name;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    private Properties properties;

    public SourceTable(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, Properties properties) {
        this.name = name;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.properties = properties;
    }

    public SourceTable() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(TypeInformation<?>[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
