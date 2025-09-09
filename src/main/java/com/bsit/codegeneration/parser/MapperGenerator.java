package com.bsit.codegeneration.parser;

import com.bsit.codegeneration.model.DatabaseConfig;
import com.bsit.codegeneration.model.TargetConfig;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MapperGenerator {

    private MapperGenerator(){

    }
    public static void generateMapper(
            String tableName, ResultSet columns, DatabaseConfig dbConfig, TargetConfig target)
            throws Exception {
        String className = toCamelCase(tableName, true);
        String recordClassName = className;
        String mapperClassName = className + "RowMapper";

        CompilationUnit cu = new CompilationUnit();
        String packageName = target.getBasePackage() + "." + target.getMapperPackage();
        cu.setPackageDeclaration(packageName);

        ClassOrInterfaceDeclaration mapperClass =
                cu.addClass(mapperClassName)
                        .setPublic(true)
                        .addImplementedType("org.jdbi.v3.core.mapper.RowMapper<" + recordClassName + ">");

        mapperClass
                .addMethod("map", Modifier.Keyword.PUBLIC)
                .addAnnotation(Override.class)
                .setType(recordClassName)
                .addParameter("ResultSet", "rs")
                .addParameter("org.jdbi.v3.core.statement.StatementContext", "ctx")
                .setBody(buildMapperBody(columns, recordClassName));

        writeToFile(target, packageName, mapperClassName, cu);
    }

    private static void writeToFile(
            TargetConfig target, String packageName, String className, CompilationUnit cu)
            throws IOException {
        String fileName = className + ".java";
        Path outputPath =
                Paths.get(target.getOutputDirectory(), packageName.replace(".", "/"), fileName);
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, cu.toString());
    }

    private static BlockStmt buildMapperBody(ResultSet columns, String recordClassName)
            throws SQLException {
        BlockStmt body = new BlockStmt();
        ResultSetMetaData metaData = columns.getMetaData();
        int columnCount = metaData.getColumnCount();

        StringBuilder constructorArgs = new StringBuilder();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            int sqlType = metaData.getColumnType(i);
            String getter = mapSqlTypeToGetter(sqlType);
            String line = getter.replace(":column", columnName);

            if (constructorArgs.length() > 0) constructorArgs.append(", ");
            constructorArgs.append(line);
        }

        ReturnStmt returnStmt =
                new ReturnStmt("return new " + recordClassName + "(" + constructorArgs + ");");
        body.addStatement(returnStmt);
        return body;
    }

    private static String mapSqlTypeToGetter(int sqlType) {
        return switch (sqlType) {
            case java.sql.Types.VARCHAR, java.sql.Types.CHAR, java.sql.Types.LONGVARCHAR ->
                    "rs.getString(\":column\")";
            case java.sql.Types.INTEGER -> "rs.getInt(\":column\")";
            case java.sql.Types.BIGINT, java.sql.Types.NUMERIC, java.sql.Types.DECIMAL ->
                    "rs.getLong(\":column\")";
            case java.sql.Types.DOUBLE, java.sql.Types.FLOAT -> "rs.getDouble(\":column\")";
            case java.sql.Types.DATE -> "rs.getDate(\":column\").toLocalDate()";
            case java.sql.Types.TIMESTAMP -> "rs.getTimestamp(\":column\").toLocalDateTime()";
            case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> "rs.getBoolean(\":column\")";
            default -> "rs.getObject(\":column\")";
        };
    }

    private static String toCamelCase(String input, boolean capitalizeFirst) {
        StringBuilder result = new StringBuilder();
        boolean capitalize = capitalizeFirst;

        for (char c : input.toCharArray()) {
            if (c == '_') {
                capitalize = true;
            } else if (capitalize) {
                result.append(Character.toUpperCase(c));
                capitalize = false;
            } else {
                result.append(Character.toLowerCase(c));
            }
        }

        return result.toString();
    }
}
