package com.bsit.codegeneration.parser;

import com.bsit.codegeneration.model.*;
import com.bsit.codegeneration.util.Relationship;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.expr.*;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.type.ClassOrInterfaceType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashSet;


public class RecordTestGenerator {

    private RecordTestGenerator(){}
    public static void generateRecordTest(
            String tableName,
            ResultSet columns,
            DatabaseConfig dbConfig,
            TargetConfig target,
            RecordConfig recordConfig,
            List<Relationship> relationships,
            List<Relationship> reverseRelationships)
            throws SQLException, IOException {

        NamingStrategyConfig naming = dbConfig.getNamingStrategy();
        String rawClassName = stripPrefix(tableName, naming.getStripPrefixes());
        String recordClassName = toCamelCase(rawClassName, naming.getUppercaseAcronyms(), true);
        String testClassName = recordClassName + "Test";

        CompilationUnit cu = new CompilationUnit();
        String packageName = target.getBasePackage() + ".test." + recordConfig.getPackageName();
        cu.setPackageDeclaration(packageName);

        // Add proper imports
        cu.addImport("org.junit.jupiter.api.Test");
        cu.addImport("org.junit.jupiter.api.DisplayName");
        cu.addImport("static org.junit.jupiter.api.Assertions.*");
        cu.addImport("com.bsit.codegeneration.record.*");

        // Import Record under test
        String recordPackage = target.getBasePackage() + "." + recordConfig.getPackageName() + "." + recordClassName;
        cu.addImport(recordPackage);

        ClassOrInterfaceDeclaration testClass = cu.addClass(testClassName).setPublic(true);

        // Extract field information using same logic as RecordGenerator
        LinkedHashMap<String, String> fieldTypes = extractRecordFieldTypes(
                columns, relationships, reverseRelationships, recordConfig, dbConfig, target);

        if (fieldTypes.isEmpty()) {
            System.out.println("Warning: No fields found for " + tableName + ". Skipping test generation.");
            return;
        }

        // Add test data constants
        addTestDataConstants(testClass, fieldTypes, cu, target, recordConfig);

        // Test parameterized constructor (records don't have default constructors)
        generateConstructorTest(testClass, recordClassName, fieldTypes);

        // Test builder if enabled
        if (recordConfig.isBuilderPattern()) {
            generateBuilderTest(testClass, recordClassName, fieldTypes);
        }

        // Test equals, hashCode, and toString (automatically generated for records)
        generateEqualsHashCodeTest(testClass, recordClassName, fieldTypes);
        generateToStringTest(testClass, recordClassName, fieldTypes);

        // Write to file
        Path outputPath = Paths.get(
                target.getOutputDirectory(),
                packageName.replace(".", "/"),
                testClassName + ".java");
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, cu.toString());
        System.out.println("Generated Record Test: " + outputPath);
    }

    private static LinkedHashMap<String, String> extractRecordFieldTypes(
            ResultSet columns,
            List<Relationship> relationships,
            List<Relationship> reverseRelationships,
            RecordConfig recordConfig,
            DatabaseConfig dbConfig,
            TargetConfig target) throws SQLException {

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        NamingStrategyConfig naming = dbConfig.getNamingStrategy();
        Set<String> seen = new HashSet<>();
        Set<String> seenRelatedTables = new HashSet<>();

        // Add fields from table columns
        while (columns.next()) {
            String colName = columns.getString("COLUMN_NAME");
            if (!seen.add(colName)) continue;

            String fieldName = toCamelCase(colName, naming.getUppercaseAcronyms(), false);
            String dbType = columns.getString("TYPE_NAME");
            String javaType = mapDbTypeToJava(dbType, colName, recordConfig.isUseJavaTime());

            fields.put(fieldName, javaType);
        }

        // Combine relationships and reverse relationships
        List<Relationship> allRelationships = new ArrayList<>();
        allRelationships.addAll(relationships);
        allRelationships.addAll(reverseRelationships);

        // Add fields for relationships, avoiding duplicates
        for (Relationship rel : allRelationships) {
            String relatedTable = rel.getRelatedTable();
            if (seenRelatedTables.contains(relatedTable)) continue;
            seenRelatedTables.add(relatedTable);

            String relatedClass = toCamelCase(stripPrefix(relatedTable, naming.getStripPrefixes()),
                    naming.getUppercaseAcronyms(), true);

            String fieldName;
            String fieldType;
            boolean isList = rel.getType() == Relationship.Type.ONE_TO_MANY || rel.getType() == Relationship.Type.MANY_TO_MANY;

            if (isList) {
                fieldName = toCamelCase(relatedTable, naming.getUppercaseAcronyms(), false) + "s";
                fieldType = "List<" + relatedClass + ">";
            } else {
                fieldName = toCamelCase(relatedTable, naming.getUppercaseAcronyms(), false);
                fieldType = relatedClass;
            }

            // Check for existing field with same name
            if (fields.containsKey(fieldName)) {
                fieldName = isList ? fieldName : fieldName + "Ref";
            }

            fields.put(fieldName, fieldType);
        }

        return fields;
    }

    private static void addTestDataConstants(
            ClassOrInterfaceDeclaration testClass,
            LinkedHashMap<String, String> fieldTypes,
            CompilationUnit cu,
            TargetConfig target,
            RecordConfig recordConfig) {
        int counter = 1;
        for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
            String field = entry.getKey();
            String type = entry.getValue();
            String constantName = field.toLowerCase();
            String testValue = getRealisticTestValue(type, field, counter++, cu, target, recordConfig);
            FieldDeclaration fd = testClass.addField(
                    type, constantName,
                    Modifier.Keyword.PRIVATE,
                    Modifier.Keyword.STATIC,
                    Modifier.Keyword.FINAL);
            fd.getVariable(0).setInitializer(testValue);
        }
    }

    private static void generateConstructorTest(
            ClassOrInterfaceDeclaration testClass,
            String recordClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testConstructor", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should create record with all parameters")));

        BlockStmt body = new BlockStmt();
        NodeList<Expression> args = new NodeList<>();
        for (String field : fieldTypes.keySet()) {
            args.add(new NameExpr(field.toLowerCase()));
        }

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, recordClassName),
                        "record",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, recordClassName), args)))));
        body.addStatement(new ExpressionStmt(new MethodCallExpr(null, "assertNotNull",
                new NodeList<>(new NameExpr("record")))));

        for (String field : fieldTypes.keySet()) {
            body.addStatement(new ExpressionStmt(new MethodCallExpr(
                    null, "assertEquals",
                    new NodeList<>(new NameExpr(field.toLowerCase()),
                            new MethodCallExpr(new NameExpr("record"), field)))));
        }
        m.setBody(body);
    }

    private static void generateBuilderTest(
            ClassOrInterfaceDeclaration testClass,
            String recordClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testBuilder", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should build record using builder pattern")));

        BlockStmt body = new BlockStmt();
        Expression builderExpr = new ObjectCreationExpr(
                null,
                new ClassOrInterfaceType(null, recordClassName + ".Builder"),
                new NodeList<>());

        for (String field : fieldTypes.keySet()) {
            builderExpr = new MethodCallExpr(
                    builderExpr,
                    field,
                    new NodeList<>(new NameExpr(field.toLowerCase())));
        }
        builderExpr = new MethodCallExpr(builderExpr, "build");

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, recordClassName),
                        "record",
                        builderExpr))));
        body.addStatement(new ExpressionStmt(new MethodCallExpr(null, "assertNotNull",
                new NodeList<>(new NameExpr("record")))));

        for (String field : fieldTypes.keySet()) {
            body.addStatement(new ExpressionStmt(new MethodCallExpr(
                    null, "assertEquals",
                    new NodeList<>(new NameExpr(field.toLowerCase()),
                            new MethodCallExpr(new NameExpr("record"), field)))));
        }

        m.setBody(body);
    }

    private static void generateEqualsHashCodeTest(
            ClassOrInterfaceDeclaration testClass,
            String recordClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testEqualsAndHashCode", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should implement equals and hashCode correctly")));

        BlockStmt body = new BlockStmt();
        NodeList<Expression> args1 = new NodeList<>();
        NodeList<Expression> args2 = new NodeList<>();
        for (String field : fieldTypes.keySet()) {
            args1.add(new NameExpr(field.toLowerCase()));
            args2.add(new NameExpr(field.toLowerCase()));
        }

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, recordClassName),
                        "record1",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, recordClassName), args1)))));
        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, recordClassName),
                        "record2",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, recordClassName), args2)))));

        body.addStatement(new ExpressionStmt(new MethodCallExpr(
                null, "assertEquals",
                new NodeList<>(new NameExpr("record1"), new NameExpr("record2")))));

        body.addStatement(new ExpressionStmt(new MethodCallExpr(
                null, "assertEquals",
                new NodeList<>(
                        new MethodCallExpr(new NameExpr("record1"), "hashCode"),
                        new MethodCallExpr(new NameExpr("record2"), "hashCode")))));

        m.setBody(body);
    }

    private static void generateToStringTest(
            ClassOrInterfaceDeclaration testClass,
            String recordClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testToString", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should have meaningful toString implementation")));

        BlockStmt body = new BlockStmt();
        NodeList<Expression> args = new NodeList<>();
        for (String field : fieldTypes.keySet()) {
            args.add(new NameExpr(field.toLowerCase()));
        }

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, recordClassName),
                        "record",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, recordClassName), args)))));

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, "String"),
                        "result",
                        new MethodCallExpr(new NameExpr("record"), "toString")))));
        body.addStatement(new ExpressionStmt(new MethodCallExpr(
                null, "assertNotNull",
                new NodeList<>(new NameExpr("result")))));
        body.addStatement(new ExpressionStmt(new MethodCallExpr(
                null, "assertTrue",
                new NodeList<>(new MethodCallExpr(new NameExpr("result"), "contains",
                        new NodeList<>(new StringLiteralExpr(recordClassName)))))));

        m.setBody(body);
    }

    // Utility methods (similar to DtoTestGenerator but adapted for records)
    private static String getRealisticTestValue(
            String type,
            String fieldName,
            int counter,
            CompilationUnit cu,
            TargetConfig target,
            RecordConfig recordConfig) {

        if (type == null) return "null";
        String lower = type.toLowerCase();

        if (type.startsWith("List<") && type.endsWith(">")) {
            String inner = type.substring(type.indexOf('<') + 1, type.lastIndexOf('>')).trim();
            cu.addImport("java.util.ArrayList");
            cu.addImport("java.util.List");
            if (target != null && recordConfig != null) {
                cu.addImport(target.getBasePackage() + "." + recordConfig.getPackageName() + "." + inner);
            }
            return "new ArrayList<>()";
        }

        // Check if it's a record type (starts with capital letter, no dot, not primitive wrappers)
        if (Character.isUpperCase(type.charAt(0)) && !type.contains(".") && !type.equals("String") && !type.equals("Integer") && !type.equals("Long")) {
            return "null"; // We'll handle record relationships separately
        }

        switch (lower) {
            case "string":
                if (fieldName.toLowerCase().contains("name")) {
                    return "\"Test Name " + counter + "\"";
                }
                if (fieldName.toLowerCase().contains("email")) {
                    return "\"test" + counter + "@example.com\"";
                }
                return "\"TestValue" + counter + "\"";

            case "integer":
                return String.valueOf(counter * 10);

            case "long":
                return (counter * 100L) + "L";

            case "java.time.localdatetime":
                cu.addImport("java.time.LocalDateTime");
                return "LocalDateTime.of(2023, " + Math.max(1, counter % 12) + ", " +
                        Math.max(1, counter % 28) + ", 10, 30, 0)";

            case "java.util.date":
                cu.addImport("java.util.Date");
                return "new Date()";

            default:
                return "null";
        }
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static String stripPrefix(String name, List<String> prefixes) {
        if (name == null || prefixes == null) return name;
        for (String prefix : prefixes) {
            if (name.startsWith(prefix)) {
                return name.substring(prefix.length());
            }
        }
        return name;
    }

    private static String toCamelCase(String name, List<String> acronyms, boolean capitalizeFirst) {
        if (name == null || name.isEmpty()) return name;

        boolean leading = name.startsWith("_");
        boolean trailing = name.endsWith("_");
        String clean = name;
        if (leading) clean = clean.substring(1);
        if (trailing) clean = clean.substring(0, clean.length() - 1);

        String[] parts = clean.toLowerCase().split("_");
        StringBuilder sb = new StringBuilder();
        if (leading) sb.append("_");

        for (int i = 0; i < parts.length; i++) {
            String p = parts[i];
            if (p.isEmpty()) continue;
            boolean isAcronym = acronyms != null && acronyms.contains(p.toUpperCase());
            if (isAcronym) {
                sb.append(p.toUpperCase());
            } else if (i == 0 && !capitalizeFirst) {
                sb.append(p);
            } else {
                sb.append(Character.toUpperCase(p.charAt(0)))
                        .append(p.substring(1));
            }
        }

        if (trailing) sb.append("_");
        return sb.toString();
    }

    private static boolean isIdField(String upperColumnName) {
        return upperColumnName.endsWith("_ID") || upperColumnName.equals("ID") || upperColumnName.endsWith("ID");
    }

    private static String mapDbTypeToJava(String dbType, String columnName, boolean useJavaTime) {
        dbType = dbType.toUpperCase();
        String upperColumnName = columnName.toUpperCase();

        return switch (dbType) {
            case "VARCHAR", "VARCHAR2", "CHAR", "TEXT", "CLOB" -> {
                if (isIdField(upperColumnName)) {
                    yield "Integer";
                } else {
                    yield "String";
                }
            }
            case "NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT" -> {
                if (isIdField(upperColumnName)) {
                    yield "Integer";
                } else {
                    yield "Long";
                }
            }
            case "DATE", "TIMESTAMP" -> useJavaTime ? "java.time.LocalDateTime" : "java.util.Date";
            default -> {
                if (isIdField(upperColumnName)) {
                    yield "Integer";
                } else {
                    yield "String";
                }
            }
        };
    }
}