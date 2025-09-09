package com.bsit.codegeneration.util;

import java.sql.*;
import java.util.*;

public final class DatabaseUtils {

    private DatabaseUtils() {}

    // Constants to avoid string literal duplication
    private static final String PKTABLE_NAME = "PKTABLE_NAME";
    private static final String FKTABLE_NAME = "FKTABLE_NAME";
    private static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
    private static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String[] TABLE_TYPES = {"TABLE"};

    public static List<String> discoverAllTables(Connection conn, String schema) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();

        try (ResultSet tables = metaData.getTables(null, schema, "%", TABLE_TYPES)) {
            while (tables.next()) {
                String tableName = tables.getString(TABLE_NAME);
                if (!isSystemTable(tableName)) {
                    tableNames.add(tableName);
                }
            }
        }
        Collections.sort(tableNames);
        return tableNames;
    }

    public static ResultSet getTableColumns(Connection conn, String schema, String tableName) throws SQLException {
        return conn.getMetaData().getColumns(null, schema, tableName, null);
    }

    public static boolean hasColumns(ResultSet columns) throws SQLException {
        return columns.isBeforeFirst() && columns.next();
    }

    public static boolean isSystemTable(String tableName) {
        String lower = tableName.toLowerCase();
        return lower.startsWith("pg_") ||
                lower.startsWith("information_schema") ||
                lower.startsWith("sql_") ||
                lower.equals("dual") ||
                lower.equals("flyway_schema_history");
    }

    public static List<Relationship> discoverRelationships(Connection conn, String schema, String tableName, boolean exported) throws SQLException {
        List<Relationship> rels = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = exported ? meta.getExportedKeys(null, schema, tableName)
                : meta.getImportedKeys(null, schema, tableName)) {
            while (rs.next()) {
                String relatedTable = exported ? rs.getString(FKTABLE_NAME) : rs.getString(PKTABLE_NAME);
                String pkColumn = rs.getString(PKCOLUMN_NAME);
                String fkColumn = rs.getString(FKCOLUMN_NAME);

                Relationship.Type relationshipType = exported ? Relationship.Type.ONE_TO_MANY : Relationship.Type.MANY_TO_ONE;

                rels.add(new Relationship(
                        relatedTable,
                        pkColumn,
                        fkColumn,
                        exported,
                        relationshipType,
                        null
                ));
            }
        }
        return rels;
    }

    public static void detectManyToMany(Connection conn, String schema, String tableName, List<Relationship> reverseRels) throws SQLException {
        for (Relationship rev : new ArrayList<>(reverseRels)) {
            String joinTable = rev.getRelatedTable();
            if (isJoinTable(conn, schema, joinTable)) {
                String targetTable = findAlternateTarget(conn, schema, joinTable, tableName);
                if (targetTable != null) {
                    reverseRels.add(new Relationship(
                            targetTable,
                            null,
                            null,
                            true,
                            Relationship.Type.MANY_TO_MANY,
                            joinTable
                    ));
                }
            }
        }
    }

    public static boolean isJoinTable(Connection conn, String schema, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet columns = meta.getColumns(null, schema, tableName, null)) {
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
            }

            try (ResultSet fks = meta.getImportedKeys(null, schema, tableName)) {
                Set<String> fkTargets = new HashSet<>();
                while (fks.next()) {
                    fkTargets.add(fks.getString(PKTABLE_NAME));
                }
                return fkTargets.size() == 2 && columnCount <= 4;
            }
        }
    }

    public static String findAlternateTarget(Connection conn, String schema, String joinTable, String currentTable) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet foreignKeys = meta.getImportedKeys(null, schema, joinTable)) {
            while (foreignKeys.next()) {
                String referencedTable = foreignKeys.getString(PKTABLE_NAME);
                if (!referencedTable.equals(currentTable)) {
                    return referencedTable;
                }
            }
        }
        return null;
    }

    // Helper method for relationship discovery
    public static RelationshipInfo extractRelationshipInfo(ResultSet rs, boolean exported) throws SQLException {
        String relatedTable = exported ? rs.getString(FKTABLE_NAME) : rs.getString(PKTABLE_NAME);
        String pkColumn = rs.getString(PKCOLUMN_NAME);
        String fkColumn = rs.getString(FKCOLUMN_NAME);
        Relationship.Type type = exported ? Relationship.Type.ONE_TO_MANY : Relationship.Type.MANY_TO_ONE;

        return new RelationshipInfo(relatedTable, pkColumn, fkColumn, type);
    }

    // Helper class to encapsulate relationship information
    public static class RelationshipInfo {
        public final String relatedTable;
        public final String pkColumn;
        public final String fkColumn;
        public final Relationship.Type type;

        public RelationshipInfo(String relatedTable, String pkColumn, String fkColumn, Relationship.Type type) {
            this.relatedTable = relatedTable;
            this.pkColumn = pkColumn;
            this.fkColumn = fkColumn;
            this.type = type;
        }
    }
}
