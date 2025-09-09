package com.bsit.codegeneration.freemarker;

import com.bsit.codegeneration.IPojoGenerator;
import com.bsit.codegeneration.metadata.DbReader;
import com.bsit.codegeneration.util.Relationship;
import com.bsit.codegeneration.util.Relationship.Type;
import com.bsit.codegeneration.util.StringUtils;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class FreeMarkerPojoGenerator implements IPojoGenerator {

    private final Configuration config;

    public FreeMarkerPojoGenerator() {
        config = new Configuration(Configuration.VERSION_2_3_31);
        config.setClassLoaderForTemplateLoading(getClass().getClassLoader(), "templates");
        config.setDefaultEncoding("UTF-8");
        config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }

    @Override
    public void generate(Connection conn, String schema, String packageName, String outputDir) throws Exception {
        DatabaseMetaData meta = conn.getMetaData();

        List<String> tables = readTableNames(meta, schema);
        Map<String, List<Relationship>> allRelationships = DbReader.readRelationships(conn, schema);

        // Build reverse relationships map
        Map<String, List<Relationship>> reverseRelationshipMap = buildReverseRelationships(allRelationships);

        // Detect join tables
        Set<String> joinTables = detectJoinTables(allRelationships, meta, schema, tables);

        // Generate for each table - PASS allRelationships as parameter
        for (String table : tables) {
            List<Relationship> relationships = allRelationships.getOrDefault(table, Collections.emptyList());
            List<Relationship> reverseRelationships = reverseRelationshipMap.getOrDefault(table, Collections.emptyList());
            generatePojoForTable(meta, schema, table, packageName, outputDir, joinTables, relationships, reverseRelationships, allRelationships);
        }
    }

    // FIXED: Added allRelationships as parameter
    private void generatePojoForTable(
            DatabaseMetaData meta,
            String schema,
            String table,
            String packageName,
            String outputDir,
            Set<String> joinTables,
            List<Relationship> relationships,
            List<Relationship> reverseRelationships,
            Map<String, List<Relationship>> allRelationships) throws Exception {

        Map<String, Object> dataModel = new HashMap<>();
        String className = toPascalCase(table);

        List<Map<String, Object>> fields = new ArrayList<>();

        // Read columns
        Map<String, String> columnTypes = new LinkedHashMap<>();
        try (ResultSet cols = meta.getColumns(null, schema, table, null)) {
            while (cols.next()) {
                String colName = cols.getString("COLUMN_NAME");
                String typeName = cols.getString("TYPE_NAME");
                String javaType = sqlTypeToJavaType(typeName);
                columnTypes.put(colName, javaType);
            }
        }

        // Build fields using provided relationships
        Set<String> addedFields = new HashSet<>();
        for (Map.Entry<String, String> entry : columnTypes.entrySet()) {
            String col = entry.getKey();
            String javaType = entry.getValue();
            boolean isRelationField = false;
            for (Relationship rel : relationships) {
                if (rel.getFkColumn().equals(col)) {
                    String refClass = toPascalCase(rel.getRelatedTable());
                    String fieldName = toCamelCase(rel.getFkColumn());
                    if (fieldName.toLowerCase().endsWith("id")) {
                        fieldName = fieldName.substring(0, fieldName.length() - 2);
                    }
                    if (addedFields.add(fieldName)) {
                        Map<String, Object> f = new HashMap<>();
                        f.put("name", fieldName);
                        f.put("type", refClass);
                        f.put("isRelation", true);
                        f.put("relationType", "ManyToOne");
                        f.put("referenceClass", refClass);
                        fields.add(f);
                    }
                    isRelationField = true;
                    break;
                }
            }
            if (!isRelationField) {
                String fieldName = toCamelCase(col);
                if (addedFields.add(fieldName)) {
                    Map<String, Object> f = new HashMap<>();
                    f.put("name", fieldName);
                    f.put("type", javaType);
                    fields.add(f);
                }
            }
        }

        // Add OneToMany from reverseRelationships
        for (Relationship rel : reverseRelationships) {
            if (rel.getType() == Type.ONE_TO_MANY && !joinTables.contains(rel.getRelatedTable())) {
                String refClass = toPascalCase(rel.getRelatedTable());
                String fieldName = toCamelCase(rel.getRelatedTable()) + "List";
                if (addedFields.add(fieldName)) {
                    Map<String, Object> f = new HashMap<>();
                    f.put("name", fieldName);
                    f.put("type", "List<" + refClass + ">");
                    f.put("isRelation", true);
                    f.put("relationType", "OneToMany");
                    f.put("referenceClass", refClass);
                    fields.add(f);
                }
            }
        }

        // Add ManyToMany from join tables - NOW allRelationships is accessible
        for (String join : joinTables) {
            List<Relationship> joinRels = allRelationships.getOrDefault(join, Collections.emptyList());
            if (joinRels.size() == DbReader.INT) {
                Relationship r1 = joinRels.get(0);
                Relationship r2 = joinRels.get(1);
                if (table.equals(r1.getRelatedTable())) {
                    String refClass = toPascalCase(r2.getRelatedTable());
                    String fieldName = toCamelCase(r2.getRelatedTable()) + "List";
                    if (addedFields.add(fieldName)) {
                        Map<String, Object> f = new HashMap<>();
                        f.put("name", fieldName);
                        f.put("type", "List<" + refClass + ">");
                        f.put("isRelation", true);
                        f.put("relationType", "ManyToMany");
                        f.put("referenceClass", refClass);
                        fields.add(f);
                    }
                } else if (table.equals(r2.getRelatedTable())) {
                    String refClass = toPascalCase(r1.getRelatedTable());
                    String fieldName = toCamelCase(r1.getRelatedTable()) + "List";
                    if (addedFields.add(fieldName)) {
                        Map<String, Object> f = new HashMap<>();
                        f.put("name", fieldName);
                        f.put("type", "List<" + refClass + ">");
                        f.put("isRelation", true);
                        f.put("relationType", "ManyToMany");
                        f.put("referenceClass", refClass);
                        fields.add(f);
                    }
                }
            }
        }

        dataModel.put("packageName", packageName + ".pojo");
        dataModel.put("className", className);
        dataModel.put("fields", fields);

        Template template = config.getTemplate("pojo.ftl");

        File outDir = new File(outputDir + "/" + packageName.replace(".", "/") + "/pojo");
        if (!outDir.exists()) outDir.mkdirs();

        File outFile = new File(outDir, className + ".java");
        try (Writer writer = new BufferedWriter(new FileWriter(outFile))) {
            template.process(dataModel, writer);
        }
        System.out.println("Generated POJO: " + outFile.getAbsolutePath());
    }

    // Helper method to build reverse relationships
    private Map<String, List<Relationship>> buildReverseRelationships(Map<String, List<Relationship>> allRelationships) {
        Map<String, List<Relationship>> reverseMap = new HashMap<>();
        for (Map.Entry<String, List<Relationship>> entry : allRelationships.entrySet()) {
            for (Relationship rel : entry.getValue()) {
                reverseMap.computeIfAbsent(rel.getRelatedTable(), k -> new ArrayList<>()).add(rel);
            }
        }
        return reverseMap;
    }

    // Helper method to detect join tables
    private Set<String> detectJoinTables(Map<String, List<Relationship>> allRelationships, DatabaseMetaData meta, String schema, List<String> tables) {
        Set<String> joinTables = new HashSet<>();
        for (String table : tables) {
            List<Relationship> rels = allRelationships.getOrDefault(table, Collections.emptyList());
            int fkCount = 0;
            try (ResultSet cols = meta.getColumns(null, schema, table, null)) {
                int totalCols = 0;
                while (cols.next()) totalCols++;
                for (Relationship r : rels) {
                    if (r.getType() == Type.MANY_TO_ONE) fkCount++;
                }
                if (fkCount == DbReader.INT && fkCount == totalCols) joinTables.add(table);
            } catch (SQLException e) {
                // Handle error
            }
        }
        return joinTables;
    }

    private List<String> readTableNames(DatabaseMetaData meta, String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    private String toCamelCase(String s) {
        return StringUtils.toCamelCase(s, Collections.emptyList(), false);
    }

    private String toPascalCase(String s) {
        return StringUtils.toCamelCase(s, Collections.emptyList(), true);
    }

    private String sqlTypeToJavaType(String sqlType) {
        if (sqlType == null) return "String";
        switch (sqlType.toUpperCase(Locale.ROOT)) {
            case "INT": case "INTEGER": return "int";
            case "BIGINT": return "long";
            case "SMALLINT": return "short";
            case "TINYINT": return "byte";
            case "FLOAT": return "float";
            case "DOUBLE": return "double";
            case "DECIMAL": case "NUMERIC": return "java.math.BigDecimal";
            case "BOOLEAN": case "BIT": return "boolean";
            case "DATE": return "java.sql.Date";
            case "TIME": return "java.sql.Time";
            case "TIMESTAMP": case "DATETIME": return "java.sql.Timestamp";
            case "TIMESTAMP_WITH_TIMEZONE": return "java.time.LocalDateTime";
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE": return "java.time.LocalDateTime";
            case "TIMESTAMPZ": return "java.time.LocalDateTime";
            case "VARCHAR": case "CHAR": case "TEXT": case "CLOB": return "String";
            case "JSON": case "JSONB": return "java.util.Map<String,Object>";
            case "UUID": return "java.util.UUID";
            case "BINARY": case "VARBINARY": case "BLOB": return "byte[]";
            default: return "String";
        }
    }
}
