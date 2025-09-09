package com.bsit.codegeneration.util;


public class FkInfo {
    private final String sourceTable;
    private final String sourceColumn;
    private final String targetTable;
    private final String targetColumn;

    public FkInfo(String sourceTable, String sourceColumn, String targetTable, String targetColumn) {
        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.targetTable = targetTable;
        this.targetColumn = targetColumn;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public String getTargetColumn() {
        return targetColumn;
    }
}

