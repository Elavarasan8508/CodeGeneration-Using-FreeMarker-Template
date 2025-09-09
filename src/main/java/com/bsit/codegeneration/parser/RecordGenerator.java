package com.bsit.codegeneration.parser;

import com.bsit.codegeneration.model.*;
import com.bsit.codegeneration.util.Relationship;
import com.bsit.codegeneration.util.StringUtils;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.stmt.BlockStmt;
import java.io.IOException;
import java.nio.file.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashSet;

public class RecordGenerator {

    private RecordGenerator(){}
    public static void generateRecord(String tableName, ResultSet columns, DatabaseConfig dbConfig,
                                      TargetConfig target, RecordConfig recordConfig,
                                      List<Relationship> relationships, List<Relationship> reverseRelationships)
            throws SQLException, IOException {

        NamingStrategyConfig naming = dbConfig.getNamingStrategy();
        String rawClassName = StringUtils.stripPrefix(tableName, naming.getStripPrefixes());
        String className = StringUtils.toCamelCase(rawClassName, naming.getUppercaseAcronyms(), true);

        CompilationUnit cu = new CompilationUnit();
        String packageName = target.getBasePackage() + "." + recordConfig.getPackageName();
        cu.setPackageDeclaration(packageName);

        cu.addImport("java.util.List");

        RecordDeclaration record = new RecordDeclaration()
                .setName(className)
                .addModifier(Modifier.Keyword.PUBLIC);
        cu.addType(record);

        Map<String, String> fields = new LinkedHashMap<>();
        Set<String> seen = new HashSet<>();
        Set<String> seenRelatedTables = new HashSet<>();

        // Add fields from table columns
        while (columns.next()) {
            String colName = columns.getString("COLUMN_NAME");
            if (!seen.add(colName)) continue;

            String fieldName = StringUtils.toCamelCase(colName, naming.getUppercaseAcronyms(), false);
            String dbType = columns.getString("TYPE_NAME");
            String javaType = mapDbTypeToJava(dbType, colName, recordConfig.isUseJavaTime());

            record.addParameter(javaType, fieldName);
            fields.put(fieldName, javaType);
        }

        // Combine relationships and reverse relationships to process together
        List<Relationship> allRelationships = new ArrayList<>();
        allRelationships.addAll(relationships);
        allRelationships.addAll(reverseRelationships);

        // Add fields for relationships, avoiding duplicates
        for (Relationship rel : allRelationships) {
            String relatedTable = rel.getRelatedTable();
            if (seenRelatedTables.contains(relatedTable)) continue; // Skip if already processed
            seenRelatedTables.add(relatedTable);

            String relatedClass = StringUtils.toCamelCase(
                    StringUtils.stripPrefix(relatedTable, naming.getStripPrefixes()),
                    naming.getUppercaseAcronyms(), true
            );
            cu.addImport(target.getBasePackage() + "." + recordConfig.getPackageName() + "." + relatedClass);

            String fieldName;
            String fieldType;
            boolean isList = rel.getType() == Relationship.Type.ONE_TO_MANY || rel.getType() == Relationship.Type.MANY_TO_MANY;

            // Determine field name and type based on relationship type
            if (isList) {
                fieldName = StringUtils.toCamelCase(relatedTable, naming.getUppercaseAcronyms(), false) + "s";
                fieldType = "List<" + relatedClass + ">";
            } else {
                fieldName = StringUtils.toCamelCase(relatedTable, naming.getUppercaseAcronyms(), false);
                fieldType = relatedClass;
            }

            // Check for existing field with same name (e.g., singular vs plural conflict)
            if (fields.containsKey(fieldName)) {
                fieldName = isList ? fieldName : fieldName + "Ref";
            }

            record.addParameter(fieldType, fieldName);
            fields.put(fieldName, fieldType);
        }

        if (recordConfig.isBuilderPattern()) {
            ClassOrInterfaceDeclaration builder = new ClassOrInterfaceDeclaration()
                    .setName("Builder")
                    .setPublic(true)
                    .setStatic(true);

            for (Map.Entry<String, String> entry : fields.entrySet()) {
                builder.addField(entry.getValue(), entry.getKey(), Modifier.Keyword.PRIVATE);
            }
            for (Map.Entry<String, String> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                String fieldType = entry.getValue();

                MethodDeclaration method = builder.addMethod(fieldName, Modifier.Keyword.PUBLIC);
                method.setType("Builder");
                method.addParameter(fieldType, fieldName);
                method.setBody(new BlockStmt()
                        .addStatement("this." + fieldName + " = " + fieldName + ";")
                        .addStatement("return this;"));
            }

            String constructorParams = String.join(", ", fields.keySet());
            MethodDeclaration build = builder.addMethod("build", Modifier.Keyword.PUBLIC);
            build.setType(className);
            build.setBody(new BlockStmt()
                    .addStatement("return new " + className + "(" + constructorParams + ");"));

            record.addMember(builder);
        }

        Path outputPath = Paths.get(target.getOutputDirectory(), packageName.replace('.', '/'), className + ".java");
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, cu.toString());

        System.out.println(" Generated Record with Builder: " + outputPath);
    }

    // Use a shared string utility class for the following methods
    // private static String stripPrefix(...)
    // private static String toCamelCase(...)

    private static boolean isIdField(String upperColumnName) {
        return upperColumnName.endsWith("_ID") || upperColumnName.equals("ID") || upperColumnName.endsWith("ID");
    }

    private static String mapDbTypeToJava(String dbType, String columnName, boolean useJavaTime) {
        dbType = dbType.toUpperCase();
        String upperColumnName = columnName.toUpperCase();

        System.out.println("Processing column: " + columnName + " (upper: " + upperColumnName + ") with type: " + dbType);

        return switch (dbType) {
            case "VARCHAR", "VARCHAR2", "CHAR", "TEXT", "CLOB" -> {
                if (isIdField(upperColumnName)) {
                    System.out.println("Mapping " + columnName + " to Integer (ID field detected in VARCHAR)");
                    yield "Integer";
                } else {
                    yield "String";
                }
            }
            case "NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT" -> {
                if (isIdField(upperColumnName)) {
                    System.out.println("Mapping " + columnName + " to Integer (ID field detected)");
                    yield "Integer";
                } else {
                    System.out.println("Mapping " + columnName + " to Long (non-ID field)");
                    yield "Long";
                }
            }
            case "DATE", "TIMESTAMP" -> useJavaTime ? "java.time.LocalDateTime" : "java.util.Date";
            default -> {
                if (isIdField(upperColumnName)) {
                    System.out.println("Mapping " + columnName + " to Integer (ID field detected in default)");
                    yield "Integer";
                } else {
                    yield "String";
                }
            }
        };
    }
}
