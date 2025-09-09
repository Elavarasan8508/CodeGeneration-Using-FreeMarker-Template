package com.bsit.codegeneration.parser;

import com.bsit.codegeneration.model.DatabaseConfig;
import com.bsit.codegeneration.model.TargetConfig;
import com.bsit.codegeneration.util.StringUtils;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;


public class RepositoryGenerator {

    private static final Logger log = LoggerFactory.getLogger(RepositoryGenerator.class);

    private RepositoryGenerator(){}

    public static void generateRepository(String tableName, ResultSet columns, DatabaseConfig dbConfig, TargetConfig target) throws SQLException, IOException {
        String rawClassName = StringUtils.stripPrefix(tableName, dbConfig.getNamingStrategy().getStripPrefixes());
        String className = StringUtils.toCamelCase(rawClassName, dbConfig.getNamingStrategy().getUppercaseAcronyms(), true);
        String daoClass = className + "Dao";
        String recordClass = className;
        String repoClass = className + "Repository";

        CompilationUnit cu = new CompilationUnit();
        String packageName = target.getBasePackage() + "." + target.getRepositoryPackage();
        cu.setPackageDeclaration(packageName);

        cu.addImport("org.jdbi.v3.core.Jdbi");
        cu.addImport("java.util.*");
        cu.addImport(target.getBasePackage() + "." + target.getDaoPackage() + "." + daoClass);
        cu.addImport(target.getBasePackage() + "." + target.getRecordPackage() + "." + recordClass);

        ClassOrInterfaceDeclaration repo = cu.addClass(repoClass).setPublic(true);
        FieldDeclaration jdbiField = repo.addField("Jdbi", "jdbi", Modifier.Keyword.PRIVATE, Modifier.Keyword.FINAL);

        ConstructorDeclaration constructor = repo.addConstructor(Modifier.Keyword.PUBLIC);
        constructor.addParameter("Jdbi", "jdbi");
        constructor.setBody(new BlockStmt().addStatement("this.jdbi = jdbi;"));

        String idField = "id";
        String idType = "Long";
        List<String> fieldNames = new ArrayList<>();

        while (columns.next()) {
            String col = columns.getString("COLUMN_NAME");
            fieldNames.add(col);
            if (idField.equalsIgnoreCase("id")) {
                idType = mapDbTypeToJava(columns.getString("TYPE_NAME"));
            }
        }

        repo.addMethod("getAll" + className + "s", Modifier.Keyword.PUBLIC)
                .setType("List<" + recordClass + ">")
                .setBody(new BlockStmt().addStatement(
                        new ReturnStmt("jdbi.withExtension(%s.class, %s -> %s.findAll())".formatted(daoClass, daoClass.toLowerCase(), daoClass.toLowerCase()))));

        MethodDeclaration findById = repo.addMethod("get" + className + "ById", Modifier.Keyword.PUBLIC);
        findById.setType("Optional<" + recordClass + ">");
        findById.addParameter(idType, "id");
        findById.setBody(new BlockStmt().addStatement(
                new ReturnStmt("jdbi.withExtension(%s.class, dao -> dao.findById(id))".formatted(daoClass))));

        MethodDeclaration create = repo.addMethod("create" + className, Modifier.Keyword.PUBLIC);
        create.setType(idType);
        create.addParameter(recordClass, "entity");
        create.setBody(new BlockStmt().addStatement(
                new ReturnStmt("jdbi.withExtension(" + daoClass + ".class, dao -> dao.insert(entity))")));

        MethodDeclaration update = repo.addMethod("update" + className, Modifier.Keyword.PUBLIC);
        update.setType("int");
        update.addParameter(recordClass, "entity");
        update.setBody(new BlockStmt().addStatement(
                new ReturnStmt("jdbi.withExtension(" + daoClass + ".class, dao -> dao.update(entity))")));

        MethodDeclaration delete = repo.addMethod("delete" + className, Modifier.Keyword.PUBLIC);
        delete.setType("void");
        delete.addParameter(idType, "id");
        delete.setBody(new BlockStmt().addStatement(
                "jdbi.useExtension(" + daoClass + ".class, dao -> dao.delete(id));"));

        Path outputPath = Paths.get(target.getOutputDirectory(), packageName.replace(".", "/"), repoClass + ".java");
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, cu.toString());

        log.info("Generated Repository: {}", outputPath);
    }

    // Use your shared StringUtils for string utilities, no method duplication.
    private static String mapDbTypeToJava(String dbType) {
        return switch (dbType.toUpperCase()) {
            case "VARCHAR", "VARCHAR2", "CHAR", "TEXT", "CLOB" -> "String";
            case "NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "INT", "INTEGER" -> "Long";
            case "DATE", "TIMESTAMP" -> "java.time.LocalDateTime";
            default -> "String";
        };
    }
}
