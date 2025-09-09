package com.bsit.codegeneration.parser;

import com.bsit.codegeneration.IPojoGenerator;
import com.bsit.codegeneration.model.*;
import com.bsit.codegeneration.util.Relationship;
import com.bsit.codegeneration.util.StringUtils;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.stmt.BlockStmt;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class PojoGenerator implements IPojoGenerator {

    private static final String BIG_DECIMAL_IMPORT = "java.math.BigDecimal";
    private static final String LOCAL_DATE = "java.time.LocalDate";
    private static final String LOCAL_DATE_TIME = "java.time.LocalDateTime";
    public static final String STRING_TYPE = "String";

    public PojoGenerator(){}

    @Override
    public void generate(Connection conn, String schema, String packageName, String outputDir) throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet tables = meta.getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                try (ResultSet columns = meta.getColumns(null, schema, tableName, "%")) {
                    List<Relationship> relationships = fetchRelationships(meta, schema, tableName);
                    List<Relationship> reverseRelationships = fetchReverseRelationships(meta, schema, tableName);

                    generatePojo(tableName, columns, new DatabaseConfig(), new TargetConfig(outputDir, packageName), new PojoConfig(), relationships, reverseRelationships);
                }
            }
        }
    }

    // Dummy implementations for relationship fetching (replace with actual from DbReader if needed)
    private List<Relationship> fetchRelationships(DatabaseMetaData meta, String schema, String tableName) throws SQLException {
        return Collections.emptyList();
    }

    private List<Relationship> fetchReverseRelationships(DatabaseMetaData meta, String schema, String tableName) throws SQLException {
        return Collections.emptyList();
    }

    public static void generatePojo(
            String tableName,
            ResultSet columns,
            DatabaseConfig dbConfig,
            TargetConfig target,
            PojoConfig pojoConfig,
            List<Relationship> relationships,
            List<Relationship> reverseRelationships
    ) throws SQLException, IOException {
        NamingStrategyConfig naming = dbConfig.getNamingStrategy();
        String rawClassName = StringUtils.stripPrefix(tableName, naming.getStripPrefixes());
        String className = StringUtils.toCamelCase(rawClassName, naming.getUppercaseAcronyms(), true);
        CompilationUnit cu = new CompilationUnit();
        String packageName = target.getBasePackage() + ".pojo";
        cu.setPackageDeclaration(packageName);
        ClassOrInterfaceDeclaration pojoClass = cu.addClass(className).setPublic(true);
        // Extract field types from columns and relationships
        LinkedHashMap<String, String> fieldTypes = extractPojoFieldTypes(
                columns, relationships, reverseRelationships, pojoConfig, dbConfig
        );
        Set<String> imports = new HashSet<>();
        fieldTypes.forEach((name, type) -> {
            pojoClass.addField(type, name, Modifier.Keyword.PRIVATE);
            if (type.contains(".")) imports.add(type);
            if (type.startsWith("List<")) imports.add("java.util.List");
        });
        generateConstructors(pojoClass, fieldTypes);
        generateAccessors(pojoClass, fieldTypes);
        if (pojoConfig.isAddBuilderPattern()) {  // Use PojoConfig
            generateBuilder(pojoClass, className, fieldTypes);
        }
        imports.forEach(cu::addImport);
        writeToFile(target, packageName, className, cu);
    }

    /**
     * Extract POJO field types (updated from DTO)
     */
    public static LinkedHashMap<String, String> extractPojoFieldTypes(
            ResultSet columns,
            List<Relationship> relationships,
            List<Relationship> reverseRelationships,
            PojoConfig pojoConfig,
            DatabaseConfig dbConfig
    ) throws SQLException {
        NamingStrategyConfig naming = dbConfig.getNamingStrategy();
        LinkedHashMap<String, String> fieldTypes = new LinkedHashMap<>();
        Set<String> addedFields = new HashSet<>();
        Set<String> relationFields = new HashSet<>();

        // Base columns
        while (columns.next()) {
            String colName = columns.getString("COLUMN_NAME");
            String fieldName = StringUtils.toCamelCase(colName, NamingStrategyConfig.getUppercaseAcronyms(), false);
            String dbType = columns.getString("TYPE_NAME");
            int colSize = columns.getInt("COLUMN_SIZE");
            int decimalDigits = columns.getInt("DECIMAL_DIGITS");
            String javaType = mapDbTypeToJava(dbType, colName, decimalDigits, colSize, "ORACLE");
            if (addedFields.add(fieldName)) {
                fieldTypes.put(fieldName, javaType);
            }
        }
        // Relationships (FK to object)
        if (pojoConfig.isIncludeRelationships() && relationships != null) {
            for (Relationship rel : relationships) {
                String relatedClass = StringUtils.toCamelCase(
                        StringUtils.stripPrefix(rel.getRelatedTable(), NamingStrategyConfig.getStripPrefixes()),
                        NamingStrategyConfig.getUppercaseAcronyms(),
                        true
                );
                String fkField = StringUtils.toCamelCase(rel.getFkColumn(), NamingStrategyConfig.getUppercaseAcronyms(), false);
                // Strip 'Id' from naming for object field
                String relationName = fkField.toLowerCase().endsWith("id")
                        ? fkField.substring(0, fkField.length() - 2)
                        : fkField;
                // Prevent duplicates
                if (!fieldTypes.containsKey(relationName) && addedFields.add(relationName)) {
                    fieldTypes.put(relationName, relatedClass);
                    relationFields.add(relationName);
                }
            }
        }
        // Reverse (collection/lists)
        if (pojoConfig.isIncludeReverseRelationships() && reverseRelationships != null) {
            for (Relationship rel : reverseRelationships) {
                String relatedClass = StringUtils.toCamelCase(
                        StringUtils.stripPrefix(rel.getRelatedTable(), NamingStrategyConfig.getStripPrefixes()),
                        NamingStrategyConfig.getUppercaseAcronyms(),
                        true
                );
                String listField = StringUtils.toCamelCase(rel.getRelatedTable(), NamingStrategyConfig.getUppercaseAcronyms(), false) + "List";
                // Ensure non-duplication against base and earlier relation names
                if (!fieldTypes.containsKey(listField) && addedFields.add(listField)) {
                    fieldTypes.put(listField, "List<" + relatedClass + ">");
                }
            }
        }
        return fieldTypes;
    }

    /**
     * Constructors
     */
    private static void generateConstructors(ClassOrInterfaceDeclaration pojoClass, Map<String, String> fieldTypes) {
        pojoClass.addConstructor(Modifier.Keyword.PUBLIC).setBody(new BlockStmt());
        ConstructorDeclaration ctor = pojoClass.addConstructor(Modifier.Keyword.PUBLIC);
        BlockStmt body = new BlockStmt();
        fieldTypes.forEach((name, type) -> {
            if (!type.startsWith("List<")) {
                ctor.addParameter(type, name);
                body.addStatement("this." + name + " = " + name + ";");
            }
        });
        ctor.setBody(body);
    }

    /**
     * Getters and Setters
     */
    private static void generateAccessors(ClassOrInterfaceDeclaration pojoClass, Map<String, String> fieldTypes) {
        fieldTypes.forEach((name, type) -> {
            pojoClass.addMethod("get" + StringUtils.capitalize(name), Modifier.Keyword.PUBLIC)
                    .setType(type)
                    .setBody(new BlockStmt().addStatement("return " + name + ";"));
            pojoClass.addMethod("set" + StringUtils.capitalize(name), Modifier.Keyword.PUBLIC)
                    .addParameter(type, name)
                    .setBody(new BlockStmt().addStatement("this." + name + " = " + name + ";"));
        });
    }

    /**
     * Builder pattern
     */
    private static void generateBuilder(ClassOrInterfaceDeclaration pojoClass, String className, Map<String, String> fieldTypes) {
        ClassOrInterfaceDeclaration builder = new ClassOrInterfaceDeclaration()
                .setName("Builder")
                .setModifiers(Modifier.Keyword.PUBLIC, Modifier.Keyword.STATIC);
        pojoClass.addMember(builder);
        FieldDeclaration instanceField = builder.addField(className, "instance", Modifier.Keyword.PRIVATE);
        instanceField.getVariable(0).setInitializer("new " + className + "()");
        fieldTypes.forEach((name, type) -> {
            MethodDeclaration method = builder.addMethod(name, Modifier.Keyword.PUBLIC)
                    .setType("Builder")
                    .addParameter(type, name);
            BlockStmt body = new BlockStmt();
            body.addStatement("instance.set" + StringUtils.capitalize(name) + "(" + name + ");");
            body.addStatement("return this;");
            method.setBody(body);
        });
        MethodDeclaration buildMethod = builder.addMethod("build", Modifier.Keyword.PUBLIC)
                .setType(className);
        BlockStmt buildBody = new BlockStmt();
        buildBody.addStatement("return instance;");
        buildMethod.setBody(buildBody);
    }

    /**
     * Write POJO to file
     */
    private static void writeToFile(TargetConfig target, String packageName, String className, CompilationUnit cu) throws IOException {
        Path outputPath = Paths.get(target.getOutputDirectory(), packageName.replace(".", "/"), className + ".java");
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, cu.toString());
        System.out.println("Generated POJO: " + outputPath);
    }

    /**
     * Map database type to Java type (improved mapping based on DAO)
     */
    private static String mapDbTypeToJava(String dbType, String columnName, int decimalDigits, int columnSize, String vendor) {
        if (columnName == null) return STRING_TYPE;
        String colNameLower = columnName.toLowerCase();

        // === Name-based overrides ===
        if (colNameLower.equals("address") || colNameLower.equals("address2")) return STRING_TYPE;
        if (colNameLower.equals("active") || colNameLower.equals("is_active") ||
                colNameLower.equals("enabled") || colNameLower.equals("is_enabled")) return "Boolean";
        if (colNameLower.contains("duration") || colNameLower.contains("length") ||
                colNameLower.equals("release_year") || colNameLower.equals("year")) return "Integer";
        if (colNameLower.equals("id") || colNameLower.endsWith("_id") || colNameLower.endsWith("_no")) return "Integer";
        if (colNameLower.contains("amount") || colNameLower.contains("price") ||
                colNameLower.contains("cost") || colNameLower.contains("rate") ||
                colNameLower.contains("fee") || colNameLower.contains("balance") ||
                colNameLower.contains("total") || colNameLower.equals("payment") ||
                colNameLower.contains("payment_amount") || colNameLower.contains("payment_total")) {
            return BIG_DECIMAL_IMPORT;
        }

        if (dbType == null) return STRING_TYPE;
        dbType = dbType.toUpperCase();

        // === Vendor-specific overrides ===
        if ("ORACLE".equals(vendor)) {
            if ("DATE".equals(dbType)) {
                // Oracle DATE includes time, but we infer usage from column name
                if (colNameLower.contains("create") || colNameLower.contains("date")) {
                    return LOCAL_DATE;
                } else if (colNameLower.contains("update") || colNameLower.contains("modified") || colNameLower.contains("timestamp")) {
                    return LOCAL_DATE_TIME;
                }
                return LOCAL_DATE_TIME; // Default for Oracle DATE
            }
        }

        // === Standard DB type mapping ===
        return switch (dbType) {
            case "SERIAL" -> "Integer";
            case "VARCHAR", "VARCHAR2", "CHAR", "TEXT", "CLOB", "LONGTEXT", "MEDIUMTEXT", "NVARCHAR2", "NCLOB", "LONG" -> STRING_TYPE;
            case "INT", "INTEGER", "SMALLINT", "TINYINT", "YEAR", "INT4", "INT2" -> {
                if ("TINYINT".equals(dbType) && columnSize == 1) yield "Boolean"; // MySQL-style boolean
                yield "Integer";
            }
            case "BIGINT", "INT8" -> "Long";
            case "DECIMAL", "NUMERIC", "NUMBER" -> BIG_DECIMAL_IMPORT;
            case "FLOAT", "REAL", "BINARY_FLOAT" -> {
                if (colNameLower.contains("amount") || colNameLower.contains("price") || colNameLower.contains("cost") || colNameLower.contains("rate")) {
                    yield BIG_DECIMAL_IMPORT;
                }
                yield "Float";
            }
            case "DOUBLE", "DOUBLE PRECISION", "BINARY_DOUBLE" -> {
                if (colNameLower.contains("amount") || colNameLower.contains("price") || colNameLower.contains("cost") || colNameLower.contains("rate")) {
                    yield BIG_DECIMAL_IMPORT;
                }
                yield "Double";
            }
            case "BOOLEAN", "BOOL", "BIT" -> "Boolean";
            case "DATE" -> LOCAL_DATE;
            case "TIME" -> "java.time.LocalTime";
            case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME" -> LOCAL_DATE_TIME;
            case "JSON", "JSONB" -> "java.util.Map<String, Object>";
            case "ARRAY" -> "java.util.List<Object>";
            case "BLOB", "BYTEA", "BINARY", "VARBINARY", "RAW" -> "byte[]";
            case "UUID" -> "java.util.UUID";
            default -> STRING_TYPE;
        };
    }
}
