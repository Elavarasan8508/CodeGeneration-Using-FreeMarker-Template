package com.bsit.codegeneration.util;

public class Relationship {

    public enum Type {
        ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY
    }

    private final String relatedTable;
    private final String pkColumn;
    private final String fkColumn;
    private final boolean reverse;
    private final Type type;
    private final String joinTableName;

    public Relationship(String relatedTable, String pkColumn, String fkColumn, boolean reverse, Type type, String joinTableName) {
        this.relatedTable = relatedTable;
        this.pkColumn = pkColumn;
        this.fkColumn = fkColumn;
        this.reverse = reverse;
        this.type = type;
        this.joinTableName = joinTableName;
    }


    public String getRelatedTable() {
        return relatedTable;
    }

    public String getPkColumn() {
        return pkColumn;
    }

    public String getFkColumn() {
        return fkColumn;
    }

    public boolean isReverse() {
        return reverse;
    }

    public Type getType() {
        return type;
    }

    public String getJoinTableName() {
        return joinTableName;
    }
}
