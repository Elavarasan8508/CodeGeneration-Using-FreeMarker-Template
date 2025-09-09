package com.bsit.codegeneration.parser;

import com.bsit.codegeneration.model.NamingStrategyConfig;
import com.bsit.codegeneration.model.TargetConfig;
import com.bsit.codegeneration.model.DatabaseConfig;
import com.bsit.codegeneration.util.Relationship;
import com.bsit.codegeneration.util.StringUtils;
import com.github.javaparser.StaticJavaParser;
import static com.github.javaparser.ParserConfiguration.LanguageLevel.*;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.VariableDeclarationExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.IfStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;
import com.github.javaparser.ast.stmt.TryStmt;
import com.github.javaparser.ast.stmt.ThrowStmt;
import com.github.javaparser.ast.stmt.ForStmt;
import com.github.javaparser.ast.stmt.WhileStmt;
import com.github.javaparser.ast.stmt.ForEachStmt;
import com.github.javaparser.ast.stmt.CatchClause;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.TypeParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Arrays;
import java.util.stream.Collectors;

public class JdbcDaoGenerator {

    public static final Logger log = LoggerFactory.getLogger(JdbcDaoGenerator.class);
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String STRING1 = "        \"\"\"";
    public static final String CONN_SET_AUTO_COMMIT_FALSE = "conn.setAutoCommit(false);";
    public static final String PS_EXECUTE_QUERY = "ps.executeQuery()";
    public static final String STRING3 = "), e);";
    public static final String OBJECT = "Object";


    private static final String INT_TYPE = "Integer";
    private static final String SQL_OBJECT = "setObject";
    private static final String SQL_GET_OBJECT = "getObject";
    private static final String FLOAT = "Float";
    private static final String DOUBLE = "Double";
    private static final String BOOLEAN = "Boolean";
    private static final String LIST_GREATER = "List<";
    private static final String SQL_EXCEPTION = "SQLException";
    private static final String CONNECTION = "Connection";
    private static final String STRING = "String";
    private static final String BIG_DECIMAL_IMPORT = "java.math.BigDecimal";
    private static final String LOCAL_DATE = "java.time.LocalDate";
    private static final String LOCAL_TIME = "java.time.LocalTime";
    private static final String LOCAL_DATE_TIME = "java.time.LocalDateTime";
    private static final String BYTE = "byte[]";
    private static final String MAP = "java.util.Map<String, Object>";
    private static final String LIST = "java.util.List<Object>";
    private static final String UID = "java.util.UUID";
    private static final String AUTO_COMMIT = "boolean autoCommit = conn.getAutoCommit();";
    private static final String COMMIT = "conn.commit();";
    private static final String PREPARED_STATEMENT = "PreparedStatement";
    private static final String BOOL = "boolean";
    private static final String TABLE = "TABLE";
    private static final String FOUR_N = "\"\"\"\n";
    private static final String FORMATTED = "formatted";
    private static final String INT_I = "int i = 0;";
    private static final String RESULT_SET = "ResultSet";
    private static final String PARAMS = "Params(ps, ";
    private static final String RS_NEXT = "rs.next()";
    private static final String INT_ARRAY = "int[]";
    private static final String BATCH = "batch";
    private static final String BRACE_NULL = ") : null";
    public static final String SELECT_COLUMNS = "SELECT_COLUMNS";
    public static final String LIST_LIST = "List<List<";
    public static final String ROLL_BACK = "conn.rollback();";
    public static final String CONN_SET_AUTO_COMMIT_AUTO_COMMIT = "conn.setAutoCommit(autoCommit);";
    public static final String LONG = "Long";
    public static final String STRING2 = " != null";


    private static class ColumnInfo {
        String name;
        String javaType;
        boolean isAutoIncrement;
        boolean isForeignKey;
        String referencedTable;
        String dbType;
        boolean isBinary;

        private static boolean isBinaryType(String dbType) {
            if (dbType == null) return false;

            String upperDbType = dbType.toUpperCase();
            return upperDbType.contains("BLOB") ||
                    "BYTEA".equals(upperDbType) ||
                    upperDbType.contains("BINARY") ||
                    upperDbType.contains("RAW");
        }


        ColumnInfo(String name, String javaType, boolean isAutoIncrement, String dbType) {
            this.name = name;
            this.javaType = javaType;
            this.isAutoIncrement = isAutoIncrement;
            this.isForeignKey = false;
            this.referencedTable = null;
            this.dbType = dbType;
            this.isBinary = isBinaryType(dbType);
        }
    }

    public static void generateDao(
            String tableName,
            ResultSet columns,
            DatabaseConfig dbConfig,
            TargetConfig target,
            List<Relationship> relationships)
            throws Exception {

        // Configure JavaParser
        StaticJavaParser.getParserConfiguration().setLanguageLevel(JAVA_16);
        // Setup configuration
        DaoGenerationConfig config = setupGenerationConfig(dbConfig, tableName);

        // Process columns and detect primary key
        ColumnProcessingResult columnResult = processColumns(columns, relationships, config.vendor);

        // Create compilation unit with imports
        CompilationUnit cu = createCompilationUnit(config, columnResult, target, relationships);

        // Generate DAO class with all methods
        generateDaoClass(cu, config, columnResult, relationships);

        // Write file
        writeToFile(target, config.packageName, config.daoClassName, cu);
    }

    private static DaoGenerationConfig setupGenerationConfig(DatabaseConfig dbConfig, String tableName) {
        String vendor = dbConfig.getDatabaseVendor() == null ? "" : dbConfig.getDatabaseVendor().toUpperCase();
        NamingStrategyConfig naming = dbConfig.getNamingStrategy();
        String rawClassName = StringUtils.stripPrefix(tableName, NamingStrategyConfig.getStripPrefixes());
        String className = StringUtils.toCamelCase(rawClassName, NamingStrategyConfig.getUppercaseAcronyms(), true);

        return new DaoGenerationConfig(
                vendor, naming, className, "Jdbc" + className + "Dao",
                "com.bsit.codegeneration.jdbcdao", tableName
        );
    }

    // Create a simple config class
    private record DaoGenerationConfig(String vendor, NamingStrategyConfig naming, String className,
                                       String daoClassName, String packageName, String tableName) {
    }

    private static ColumnProcessingResult processColumns(ResultSet columns, List<Relationship> relationships, String vendor)
            throws SQLException {
        List<ColumnInfo> columnsInfo = new ArrayList<>();
        Map<String, String> foreignKeyMap = buildForeignKeyMap(relationships);

        // Process each column
        while (columns.next()) {
            ColumnInfo colInfo = processColumn(columns, foreignKeyMap, vendor);
            columnsInfo.add(colInfo);
        }

        // Detect primary key
        PrimaryKeyInfo pkInfo = detectPrimaryKey(columnsInfo);

        return new ColumnProcessingResult(columnsInfo, pkInfo);
    }

    private static Map<String, String> buildForeignKeyMap(List<Relationship> relationships) {
        Map<String, String> foreignKeyMap = new HashMap<>();
        if (relationships != null) {
            for (Relationship rel : relationships) {
                if (rel.getType() == Relationship.Type.MANY_TO_ONE) {
                    foreignKeyMap.put(rel.getFkColumn(), rel.getRelatedTable());
                }
            }
        }
        return foreignKeyMap;
    }

    private static ColumnInfo processColumn(ResultSet columns, Map<String, String> foreignKeyMap, String vendor)
            throws SQLException {
        String colName = columns.getString("COLUMN_NAME");
        String dbType = columns.getString("TYPE_NAME");
        int decimalDigits = getIntSafely(columns, "DECIMAL_DIGITS");

        String javaType = mapDbTypeToJava(dbType, colName, decimalDigits, vendor);
        boolean isAutoIncrement = determineAutoIncrement(columns, dbType);

        ColumnInfo colInfo = new ColumnInfo(colName, javaType, isAutoIncrement, dbType);

        // Set foreign key info
        if (foreignKeyMap.containsKey(colName)) {
            colInfo.isForeignKey = true;
            colInfo.referencedTable = foreignKeyMap.get(colName);
        }

        return colInfo;
    }

    private static int getIntSafely(ResultSet columns, String columnName) {
        try {
            return columns.getInt(columnName);
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static boolean determineAutoIncrement(ResultSet columns, String dbType) {
        try {
            String isAutoStr = columns.getString("IS_AUTOINCREMENT");
            if ("YES".equals(isAutoStr)) return true;
        } catch (SQLException ignored) {
            // Continue to check SERIAL
        }
        return dbType != null && dbType.equalsIgnoreCase("SERIAL");
    }

    private static PrimaryKeyInfo detectPrimaryKey(List<ColumnInfo> columnsInfo) {
        String pkField = null;
        String idType = null;
        boolean pkIsAuto = false;

        // Try to find primary key using naming patterns
        for (ColumnInfo colInfo : columnsInfo) {
            String colName = colInfo.name;
            if (isPrimaryKeyCandidate(colName, colInfo)) {
                pkField = colName;
                idType = colInfo.javaType;
                pkIsAuto = colInfo.isAutoIncrement;
                break;
            }
        }

        // Fallback to first column or default
        if (pkField == null) {
            if (!columnsInfo.isEmpty()) {
                ColumnInfo first = columnsInfo.getFirst();
                pkField = first.name;
                idType = first.javaType;
                pkIsAuto = first.isAutoIncrement;
            } else {
                pkField = "id";
                idType = INT_TYPE;
            }
        }

        idType = mapJavaTypeToWrapper(idType);

        return new PrimaryKeyInfo(pkField, idType, pkIsAuto);
    }

    private static String mapJavaTypeToWrapper(String type) {
        return switch (type) {
            case "int" -> INT_TYPE;
            case "long" -> "Long";
            case "float" -> FLOAT;
            case "double" -> DOUBLE;
            case BOOL -> BOOLEAN;
            default -> type;
        };
    }

    private static boolean isPrimaryKeyCandidate(String colName, ColumnInfo colInfo) {
        String lower = colName.toLowerCase();
        if (lower.equals("id")) return true;
        if (lower.endsWith("_id") && !colInfo.isForeignKey) return true;
        if (lower.endsWith("_no")) return true;
        return lower.contains("id") && !colInfo.isForeignKey;
    }

    private static CompilationUnit createCompilationUnit(DaoGenerationConfig config,
                                                         ColumnProcessingResult columnResult, TargetConfig target, List<Relationship> relationships) {
        CompilationUnit cu = new CompilationUnit();
        cu.setPackageDeclaration(config.packageName);

        addEssentialImports(cu);
        addConditionalImports(cu, columnResult.columnsInfo);
        addModelImports(cu, target, config.className);
        addRelationshipImports(cu, target, relationships, columnResult.columnsInfo);

        return cu;
    }

    private static void addEssentialImports(CompilationUnit cu) {
        cu.addImport("java.sql.Connection");
        cu.addImport("java.sql.SQLException");
        cu.addImport("java.sql.ResultSet");
        cu.addImport("java.sql.PreparedStatement");
        cu.addImport("java.sql.Statement");
        cu.addImport("java.sql.Types");
        cu.addImport("java.util.ArrayList");
        cu.addImport("java.util.List");
        cu.addImport("org.slf4j.Logger");
        cu.addImport("org.slf4j.LoggerFactory");
    }

    private static void addConditionalImports(CompilationUnit cu, List<ColumnInfo> columnsInfo) {
        if (hasFieldsOfType(columnsInfo, LOCAL_DATE_TIME)) {
            cu.addImport("java.sql.Timestamp");
        }
        if (hasFieldsOfType(columnsInfo, LOCAL_DATE)) {
            cu.addImport(LOCAL_DATE);
        }
        if (hasFieldsOfType(columnsInfo, LOCAL_TIME)) {
            cu.addImport(LOCAL_TIME);
        }
        if (hasFieldsOfType(columnsInfo, BIG_DECIMAL_IMPORT)) {
            cu.addImport(BIG_DECIMAL_IMPORT);
        }
    }

    private static boolean hasFieldsOfType(List<ColumnInfo> columnsInfo, String javaType) {
        return columnsInfo.stream().anyMatch(c -> javaType.equals(c.javaType));
    }

    private static void addModelImports(CompilationUnit cu, TargetConfig target, String className) {
        cu.addImport(target.getBasePackage() + ".pojo." + className);
    }

    private static void addRelationshipImports(CompilationUnit cu, TargetConfig target,
                                               List<Relationship> relationships, List<ColumnInfo> columnsInfo) {

        Set<String> relatedImports = new HashSet<>();
        if (relationships != null) {
            for (Relationship rel : relationships) {
                if (rel.getType() == Relationship.Type.MANY_TO_ONE) {
                    // Only import if this column is actually a FK in the current table
                    boolean actuallyUsed = columnsInfo.stream()
                            .anyMatch(c -> c.isForeignKey && c.referencedTable.equals(rel.getRelatedTable()));

                    if (actuallyUsed) {
                        String rawRelatedTable = rel.getRelatedTable();
                        String rawRelatedClassName = StringUtils.stripPrefix(rawRelatedTable, NamingStrategyConfig.getStripPrefixes());
                        String relatedClassName = StringUtils.toCamelCase(rawRelatedClassName, NamingStrategyConfig.getUppercaseAcronyms(), true);
                        relatedImports.add(target.getBasePackage() + ".pojo." + relatedClassName);
                    }
                }
            }
        }

        // Add imports for related POJOs only if they're actually used
        for (String importStmt : relatedImports) {
            cu.addImport(importStmt);
        }
    }

    private static void generateDaoClass(CompilationUnit cu, DaoGenerationConfig config,
                                         ColumnProcessingResult columnResult, List<Relationship> relationships) {

        ClassOrInterfaceDeclaration daoClass = cu.addClass(config.daoClassName, Modifier.Keyword.PUBLIC);

        // Add logger
        addLogger(daoClass, config.daoClassName);

        // Create constants
        createConstants(daoClass, config.tableName, columnResult.columnsInfo,
                columnResult.pkInfo.field, columnResult.pkInfo.isAuto);

        // Add utility methods
        createUtilityMethods(daoClass);

        // Create all DAO methods
        createAllDaoMethods(daoClass, config, columnResult, relationships);
    }

    private static void addLogger(ClassOrInterfaceDeclaration daoClass, String daoClassName) {
        daoClass.addFieldWithInitializer("Logger", "LOGGER",
                StaticJavaParser.parseExpression("LoggerFactory.getLogger(" + daoClassName + ".class)"),
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);
    }

    private static void createConstants(
            ClassOrInterfaceDeclaration daoClass,
            String tableName,
            List<ColumnInfo> columnsInfo,
            String pkField,
            boolean pkIsAuto) {

        // TABLE constant
        daoClass.addFieldWithInitializer(STRING, TABLE,
                new StringLiteralExpr(tableName),
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);

        // COL_ constants
        for (ColumnInfo c : columnsInfo) {
            String colConstantName = "COL_" + c.name.toUpperCase();
            daoClass.addFieldWithInitializer(STRING, colConstantName,
                    new StringLiteralExpr(c.name),
                    Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);
        }

        // SELECT_COLUMNS constant - NEW: Create constant for column list to avoid repetition
        String selectColumns = columnsInfo.stream()
                .map(c -> c.name).collect(Collectors.joining(", "));

        daoClass.addFieldWithInitializer(STRING, SELECT_COLUMNS,
                new StringLiteralExpr(selectColumns),
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);

        // INSERT_SQL
        List<ColumnInfo> insertCols = columnsInfo.stream()
                .filter(c -> !(pkIsAuto && c.name.equals(pkField)))
                .collect(Collectors.toList());
        String insertColumnsStr = insertCols.stream()
                .map(c -> "%s")
                .collect(Collectors.joining(", "));
        String insertPlaceholders = insertCols.stream().map(c -> "?").collect(Collectors.joining(", "));
        String insertSqlBuilder = FOUR_N +
                "        INSERT INTO %s (" + insertColumnsStr + ")\n" +
                "        VALUES (" + insertPlaceholders + ")\n" +
                STRING1;

        MethodCallExpr insertSqlExpr = new MethodCallExpr(
                StaticJavaParser.parseExpression(insertSqlBuilder), FORMATTED);
        insertSqlExpr.addArgument(new NameExpr(TABLE));
        for (ColumnInfo c : insertCols) {
            insertSqlExpr.addArgument(new NameExpr("COL_" + c.name.toUpperCase()));
        }
        daoClass.addFieldWithInitializer(STRING, "INSERT_SQL", insertSqlExpr,
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);

        // UPDATE_SQL (exclude immutable fields like EFFECTIVE_DATE if needed; assuming all updatable for generality)
        List<ColumnInfo> updateCols = columnsInfo.stream()
                .filter(c -> !c.name.equalsIgnoreCase(pkField))
                .collect(Collectors.toList());

        String setClause = updateCols.stream().map(c -> "%s = ?").collect(Collectors.joining(", "));

        String updateBuilder = FOUR_N +
                "        UPDATE %s\n" +
                "        SET " + setClause + "\n" +
                "        WHERE %s = ?\n" +
                STRING1;
        MethodCallExpr updateSqlExpr = new MethodCallExpr(
                StaticJavaParser.parseExpression(updateBuilder), FORMATTED);
        updateSqlExpr.addArgument(new NameExpr(TABLE));
        for (ColumnInfo c : updateCols) {
            updateSqlExpr.addArgument(new NameExpr("COL_" + c.name.toUpperCase()));
        }
        updateSqlExpr.addArgument(new NameExpr("COL_" + pkField.toUpperCase()));
        daoClass.addFieldWithInitializer(STRING, "UPDATE_SQL", updateSqlExpr,
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);

        // DELETE_SQL
        String deleteBuilder = FOUR_N +
                "        DELETE FROM %s WHERE %s = ?\n" +
                STRING1;

        MethodCallExpr deleteSqlExpr = new MethodCallExpr(
                StaticJavaParser.parseExpression(deleteBuilder), FORMATTED);
        deleteSqlExpr.addArgument(new NameExpr(TABLE));
        deleteSqlExpr.addArgument(new NameExpr("COL_" + pkField.toUpperCase()));

        daoClass.addFieldWithInitializer(STRING, "DELETE_SQL", deleteSqlExpr,
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);

        // Batch size constants
        daoClass.addFieldWithInitializer("int", "INSERT_BATCH_SIZE",
                StaticJavaParser.parseExpression("500"),
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);

        daoClass.addFieldWithInitializer("int", "UPDATE_BATCH_SIZE",
                StaticJavaParser.parseExpression("500"),
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);

        daoClass.addFieldWithInitializer("int", "DELETE_CHUNK_SIZE",
                StaticJavaParser.parseExpression("1000"),
                Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC, Modifier.Keyword.FINAL);

    }

    private static void createUtilityMethods(ClassOrInterfaceDeclaration daoClass) {
        // Utility to chunk lists (for IN clauses and batches)
        MethodDeclaration chunkList = daoClass.addMethod("chunkList", Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC);
        chunkList.addTypeParameter(new TypeParameter("T"));
        chunkList.setType("List<List<T>>");
        chunkList.addParameter("List<T>", "list");
        chunkList.addParameter("int", "chunkSize");
        BlockStmt chunkBlock = new BlockStmt();
        chunkBlock.addStatement(StaticJavaParser.parseStatement("List<List<T>> chunks = new ArrayList<>();"));

        ForStmt forStmt = new ForStmt();
        NodeList<Expression> initialization = new NodeList<>();
        initialization.add(StaticJavaParser.parseStatement(INT_I).asExpressionStmt().getExpression());
        forStmt.setInitialization(initialization);
        forStmt.setCompare(StaticJavaParser.parseExpression("i < list.size()"));
        NodeList<Expression> update = new NodeList<>();
        update.add(StaticJavaParser.parseExpression("i += chunkSize"));
        forStmt.setUpdate(update);
        BlockStmt forBody = new BlockStmt();
        forBody.addStatement(StaticJavaParser.parseStatement("chunks.add(list.subList(i, Math.min(i + chunkSize, list.size())));"));
        forStmt.setBody(forBody);
        chunkBlock.addStatement(forStmt);

        chunkBlock.addStatement(StaticJavaParser.parseStatement("return chunks;"));
        chunkList.setBody(chunkBlock);

        // NEW: Add getSelectByColumnSql to avoid duplicating SELECT query strings
        MethodDeclaration selectByCol = daoClass.addMethod("getSelectByColumnSql", Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC);
        selectByCol.setType(STRING);
        selectByCol.addParameter(STRING, "column");
        BlockStmt selectBody = new BlockStmt();
        String selectBuilder = FOUR_N +
                "        SELECT %s FROM %s WHERE %s = ?\n" +
                STRING1;
        MethodCallExpr selectExpr = new MethodCallExpr(
                StaticJavaParser.parseExpression(selectBuilder), FORMATTED);
        selectExpr.addArgument(new NameExpr(SELECT_COLUMNS));
        selectExpr.addArgument(new NameExpr(TABLE));
        selectExpr.addArgument(new NameExpr("column"));
        selectBody.addStatement(new ReturnStmt(selectExpr));
        selectByCol.setBody(selectBody);

        // NEW: Add setNullable helper to handle null values uniformly
        MethodDeclaration setNullable = daoClass.addMethod("setNullable", Modifier.Keyword.PRIVATE, Modifier.Keyword.STATIC);
        setNullable.setType("void");
        setNullable.addParameter(PREPARED_STATEMENT, "ps");
        setNullable.addParameter("int", "index");
        setNullable.addParameter(OBJECT, "value");
        setNullable.addParameter("int", "sqlType");
        setNullable.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));
        BlockStmt nullableBody = new BlockStmt();
        nullableBody.addStatement(new IfStmt(
                StaticJavaParser.parseExpression("value != null"),
                StaticJavaParser.parseStatement("ps.setObject(index, value, sqlType);"),
                StaticJavaParser.parseStatement("ps.setNull(index, sqlType);")
        ));
        setNullable.setBody(nullableBody);


        // the toTimestamp helper method for set params method
        MethodDeclaration toTimestampMethod = daoClass.addMethod("toTimestamp", Modifier.Keyword.PUBLIC, Modifier.Keyword.STATIC);
        toTimestampMethod.setType(OBJECT);
        toTimestampMethod.addParameter(OBJECT, "dateTime");

        BlockStmt toTimestampBlock = new BlockStmt();
        IfStmt ifStmt = new IfStmt();
        ifStmt.setCondition(StaticJavaParser.parseExpression("dateTime instanceof java.time.LocalDateTime localDateTime"));
        BlockStmt thenBlock = new BlockStmt();
        thenBlock.addStatement(StaticJavaParser.parseStatement("return java.sql.Timestamp.valueOf(localDateTime);"));
        ifStmt.setThenStmt(thenBlock);
        toTimestampBlock.addStatement(ifStmt);
        toTimestampBlock.addStatement(StaticJavaParser.parseStatement("return dateTime;"));
        toTimestampMethod.setBody(toTimestampBlock);


    }

    private static void createAllDaoMethods(ClassOrInterfaceDeclaration daoClass, DaoGenerationConfig config,
                                            ColumnProcessingResult columnResult, List<Relationship> relationships) {

        String modelClassName = config.className;
        String pkField = columnResult.pkInfo.field;
        String idType = columnResult.pkInfo.type;
        boolean pkIsAuto = columnResult.pkInfo.isAuto;
        List<ColumnInfo> columnsInfo = columnResult.columnsInfo;

        // Insert methods
        createInsertMethod(daoClass, modelClassName, pkField, idType, pkIsAuto);
        createBulkInsertMethod(daoClass, modelClassName);
        createIsInvalidListMethod(daoClass, modelClassName);
        createProcessBatchMethod(daoClass, modelClassName, pkField, idType, pkIsAuto);

        // Find methods
        createFindByIdMethod(daoClass, modelClassName, pkField, idType);
        createFindAllMethod(daoClass, modelClassName,columnResult);

        // Update methods
        createUpdateMethod(daoClass, modelClassName, pkField, idType, columnsInfo);
        createBulkUpdateMethod(daoClass, modelClassName);
        createIsInvalidUpdateListMethod(daoClass, modelClassName, pkField);
        createProcessUpdateBatchMethod(daoClass, modelClassName, pkField, idType, columnsInfo);

        // Delete methods
        createDeleteMethod(daoClass, idType);
        createBulkDeleteMethod(daoClass, idType);
        createIsInvalidIdsListMethod(daoClass, idType);
        createProcessDeleteChunkMethod(daoClass, pkField, idType);

        // Relationship methods
        createRelationshipMethods(daoClass, modelClassName, columnsInfo, relationships);

        // Utility methods
        createSetParamsMethod(daoClass, modelClassName, columnsInfo, pkField, pkIsAuto);
        createExtractMethod(daoClass, modelClassName, columnsInfo);
    }

    // Result classes
    private record ColumnProcessingResult(List<ColumnInfo> columnsInfo, PrimaryKeyInfo pkInfo) {
    }

    private record PrimaryKeyInfo(String field, String type, boolean isAuto) {
    }

    private static void createInsertMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            String pkField,
            String idType,
            boolean pkIsAuto) {

        MethodDeclaration m = daoClass.addMethod("insert", Modifier.Keyword.PUBLIC);
        m.setType(mapJavaTypeToMethod(idType));
        m.addParameter(CONNECTION, "conn");
        m.addParameter(modelClassName, StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false));
        // REMOVED: m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));  // No longer throws checked SQLException

        String paramName = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false);

        BlockStmt block = new BlockStmt();
        block.addStatement(StaticJavaParser.parseStatement("LOGGER.debug(\"Inserting " + modelClassName.toLowerCase() + ": {}\", " + paramName + ");"));
        TryStmt tryStmt = new TryStmt();
        BlockStmt tryBlock = new BlockStmt();

        Expression psInitializer = pkIsAuto ?
                StaticJavaParser.parseExpression("conn.prepareStatement(INSERT_SQL, Statement.RETURN_GENERATED_KEYS)") :
                StaticJavaParser.parseExpression("conn.prepareStatement(INSERT_SQL)");

        VariableDeclarator psDeclarator = new VariableDeclarator();
        psDeclarator.setName("ps");
        psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
        psDeclarator.setInitializer(psInitializer);
        tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

        tryBlock.addStatement(StaticJavaParser.parseStatement("set" + modelClassName + PARAMS + paramName + ");"));

        tryBlock.addStatement(StaticJavaParser.parseStatement("ps.executeUpdate();"));

        if (pkIsAuto) {
            TryStmt innerTryStmt = new TryStmt();
            BlockStmt innerTryBlock = new BlockStmt();

            VariableDeclarator rsDeclarator = new VariableDeclarator();
            rsDeclarator.setName("rs");
            rsDeclarator.setType(new ClassOrInterfaceType(null, RESULT_SET));
            rsDeclarator.setInitializer(StaticJavaParser.parseExpression("ps.getGeneratedKeys()"));
            innerTryStmt.getResources().add(new VariableDeclarationExpr(rsDeclarator));

            BlockStmt ifBlock = new BlockStmt();
            ifBlock.addStatement(StaticJavaParser.parseStatement(idType + " id = rs." + getResultSetGetter(idType) + "(1);"));
            ifBlock.addStatement(StaticJavaParser.parseStatement(
                    paramName + ".set" + StringUtils.toCamelCase(pkField, NamingStrategyConfig.getUppercaseAcronyms(), true) + "(id);"));
            ifBlock.addStatement(new ReturnStmt(new NameExpr("id")));

            BlockStmt elseBlock = new BlockStmt();
            elseBlock.addStatement(StaticJavaParser.parseStatement("LOGGER.error(\"Failed to retrieve generated ID for inserted " + modelClassName.toLowerCase() + "\");"));
            // CHANGED: Use DataAccessException instead of SQLException
            elseBlock.addStatement(new ThrowStmt(StaticJavaParser.parseExpression("new DataAccessException(\"Failed to retrieve generated ID for inserted " + modelClassName.toLowerCase() + "\")")));

            innerTryBlock.addStatement(new IfStmt(
                    StaticJavaParser.parseExpression(RS_NEXT), ifBlock, elseBlock));
            innerTryStmt.setTryBlock(innerTryBlock);
            tryBlock.addStatement(innerTryStmt);
        } else {
            tryBlock.addStatement(new ReturnStmt(StaticJavaParser.parseExpression(paramName + ".get" + StringUtils.toCamelCase(pkField, NamingStrategyConfig.getUppercaseAcronyms(), true) + "()")));
        }

        tryStmt.setTryBlock(tryBlock);

        // NEW: Add catch clause to handle SQLException and rethrow as unchecked DataAccessException
        CatchClause catchClause = new CatchClause();
        catchClause.setParameter(new Parameter(new ClassOrInterfaceType(null, SQL_EXCEPTION), "e"));
        BlockStmt catchBlock = new BlockStmt();
        catchBlock.addStatement(StaticJavaParser.parseStatement(
                "throw new DataAccessException(\"Failed to insert " + modelClassName.toLowerCase() + ": \" + " + paramName + ", e);"));
        catchClause.setBody(catchBlock);
        tryStmt.getCatchClauses().add(catchClause);

        block.addStatement(tryStmt);

        m.setBody(block);
    }


    private static void createBulkInsertMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName) {

        MethodDeclaration m = daoClass.addMethod("insertAll", Modifier.Keyword.PUBLIC);
        m.setType(INT_ARRAY);
        m.addParameter(CONNECTION, "conn");
        m.addParameter(LIST_GREATER + modelClassName + ">", StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false) + "s");

        String paramName = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false) + "s";
        String validationMethodName = "isInvalid" + modelClassName + "List";
        String processBatchMethodName = "processBatch";

        BlockStmt block = new BlockStmt();

        // Input validation using helper method
        block.addStatement(new IfStmt(
                StaticJavaParser.parseExpression(validationMethodName + "(" + paramName + ")"),
                new BlockStmt().addStatement(new ReturnStmt(StaticJavaParser.parseExpression("new int[0]"))),
                null
        ));

        // Setup batch processing
        block.addStatement(StaticJavaParser.parseStatement("int batchSize = INSERT_BATCH_SIZE;"));
        block.addStatement(StaticJavaParser.parseStatement(LIST_LIST + modelClassName + ">> batches = chunkList(" + paramName + ", batchSize);"));
        block.addStatement(StaticJavaParser.parseStatement("int[] totalResults = new int[" + paramName + ".size()];"));
        block.addStatement(StaticJavaParser.parseStatement("int resultIndex = 0;"));

        // Transaction management
        block.addStatement(StaticJavaParser.parseStatement(AUTO_COMMIT));
        TryStmt outerTry = new TryStmt();
        BlockStmt outerTryBlock = new BlockStmt();
        outerTryBlock.addStatement(StaticJavaParser.parseStatement(CONN_SET_AUTO_COMMIT_FALSE));

        // Process batches using helper method
        ForEachStmt batchLoop = new ForEachStmt();
        batchLoop.setVariable(new VariableDeclarationExpr(new ClassOrInterfaceType(null, LIST_GREATER + modelClassName + ">"), BATCH));
        batchLoop.setIterable(new NameExpr("batches"));
        BlockStmt batchBody = new BlockStmt();
        batchBody.addStatement(StaticJavaParser.parseStatement("int[] results = " + processBatchMethodName + "(conn, batch);"));
        batchBody.addStatement(StaticJavaParser.parseStatement("System.arraycopy(results, 0, totalResults, resultIndex, results.length);"));
        batchBody.addStatement(StaticJavaParser.parseStatement("resultIndex += results.length;"));
        batchLoop.setBody(batchBody);
        outerTryBlock.addStatement(batchLoop);

        outerTryBlock.addStatement(StaticJavaParser.parseStatement(COMMIT));
        outerTry.setTryBlock(outerTryBlock);

        // Error handling
        CatchClause catchClause = new CatchClause();
        catchClause.setParameter(new Parameter(new ClassOrInterfaceType(null, SQL_EXCEPTION), "e"));
        BlockStmt catchBlock = new BlockStmt();
        catchBlock.addStatement(StaticJavaParser.parseStatement(ROLL_BACK));  // Keep rollback if present
        catchBlock.addStatement(StaticJavaParser.parseStatement(
                "throw new DataAccessException(\"Failed to insert all " + modelClassName.toLowerCase() + "s in batch\", e);"));
        catchClause.setBody(catchBlock);
        outerTry.getCatchClauses().add(catchClause);

        // Finally block
        BlockStmt finallyBlock = new BlockStmt();
        finallyBlock.addStatement(StaticJavaParser.parseStatement(CONN_SET_AUTO_COMMIT_AUTO_COMMIT));
        outerTry.setFinallyBlock(finallyBlock);

        block.addStatement(outerTry);
        block.addStatement(StaticJavaParser.parseStatement("return totalResults;"));
        m.setBody(block);
    }

    private static void createIsInvalidListMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName) {

        String methodName = "isInvalid" + modelClassName + "List";
        String paramName = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false) + "s";

        MethodDeclaration m = daoClass.addMethod(methodName, Modifier.Keyword.PRIVATE);
        m.setType(BOOL);
        m.addParameter(LIST_GREATER + modelClassName + ">", paramName);

        BlockStmt block = new BlockStmt();

        // Check for null or empty list
        block.addStatement(new IfStmt(
                StaticJavaParser.parseExpression(paramName + " == null || " + paramName + ".isEmpty()"),
                new BlockStmt().addStatement(new ReturnStmt(StaticJavaParser.parseExpression("true"))),
                null
        ));

        // Check for null elements
        ForStmt nullCheckFor = new ForStmt();
        NodeList<Expression> nullInit = new NodeList<>();
        nullInit.add(StaticJavaParser.parseStatement(INT_I).asExpressionStmt().getExpression());
        nullCheckFor.setInitialization(nullInit);
        nullCheckFor.setCompare(StaticJavaParser.parseExpression("i < " + paramName + ".size()"));
        NodeList<Expression> nullUpdate = new NodeList<>();
        nullUpdate.add(StaticJavaParser.parseExpression("i++"));
        nullCheckFor.setUpdate(nullUpdate);

        BlockStmt nullBody = new BlockStmt();
        nullBody.addStatement(new IfStmt(
                StaticJavaParser.parseExpression(paramName + ".get(i) == null"),
                new ThrowStmt(StaticJavaParser.parseExpression("new IllegalArgumentException(\"Null DTO at index \" + i + \" in batch insert\")")),
                null
        ));
        nullCheckFor.setBody(nullBody);
        block.addStatement(nullCheckFor);

        // Return false if validation passes
        block.addStatement(new ReturnStmt(StaticJavaParser.parseExpression(FALSE)));
        m.setBody(block);
    }

    private static void createProcessBatchMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            String pkField,
            String idType,
            boolean pkIsAuto) {

        String methodName = "processBatch";
        String className = modelClassName.replace("DTO", "");
        String itemVar = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false);
        String pkSetter = ".set" + StringUtils.toCamelCase(pkField, NamingStrategyConfig.getUppercaseAcronyms(), true);

        MethodDeclaration m = daoClass.addMethod(methodName, Modifier.Keyword.PRIVATE);
        m.setType(INT_ARRAY);
        m.addParameter(CONNECTION, "conn");
        m.addParameter(LIST_GREATER + modelClassName + ">", BATCH);
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        BlockStmt block = new BlockStmt();

        TryStmt tryStmt = new TryStmt();
        BlockStmt tryBlock = new BlockStmt();

        // Create PreparedStatement
        Expression psInitializer = pkIsAuto ?
                StaticJavaParser.parseExpression("conn.prepareStatement(INSERT_SQL, Statement.RETURN_GENERATED_KEYS)") :
                StaticJavaParser.parseExpression("conn.prepareStatement(INSERT_SQL)");

        VariableDeclarator psDeclarator = new VariableDeclarator();
        psDeclarator.setName("ps");
        psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
        psDeclarator.setInitializer(psInitializer);
        tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

        // Add batch parameters
        ForEachStmt innerLoop = new ForEachStmt();
        innerLoop.setVariable(new VariableDeclarationExpr(new ClassOrInterfaceType(null, modelClassName), itemVar));
        innerLoop.setIterable(new NameExpr(BATCH));
        BlockStmt innerBody = new BlockStmt();
        innerBody.addStatement(StaticJavaParser.parseStatement("set" + className + PARAMS + itemVar + ");"));
        innerBody.addStatement(StaticJavaParser.parseStatement("ps.addBatch();"));
        innerLoop.setBody(innerBody);
        tryBlock.addStatement(innerLoop);

        // Execute batch
        tryBlock.addStatement(StaticJavaParser.parseStatement("int[] results = ps.executeBatch();"));
        tryBlock.addStatement(StaticJavaParser.parseStatement("LOGGER.debug(\"Inserted {} rows in batch\", results.length);"));

        // Handle generated keys if auto-increment
        if (pkIsAuto) {
            String keyRetrieval = getResultSetGetter(idType) + "(1)";

            TryStmt innerTryStmt = new TryStmt();
            BlockStmt innerTryBlock = new BlockStmt();

            VariableDeclarator rsDeclarator = new VariableDeclarator();
            rsDeclarator.setName("rs");
            rsDeclarator.setType(new ClassOrInterfaceType(null, RESULT_SET));
            rsDeclarator.setInitializer(StaticJavaParser.parseExpression("ps.getGeneratedKeys()"));
            innerTryStmt.getResources().add(new VariableDeclarationExpr(rsDeclarator));

            innerTryBlock.addStatement(StaticJavaParser.parseStatement(LIST_GREATER + idType + "> keys = new ArrayList<>();"));
            WhileStmt whileStmt = new WhileStmt();
            whileStmt.setCondition(StaticJavaParser.parseExpression(RS_NEXT));
            BlockStmt whileBody = new BlockStmt();
            whileBody.addStatement(StaticJavaParser.parseStatement("keys.add(rs." + keyRetrieval + ");"));
            whileStmt.setBody(whileBody);
            innerTryBlock.addStatement(whileStmt);

            ForStmt forStmt = new ForStmt();
            NodeList<Expression> initialization = new NodeList<>();
            initialization.add(StaticJavaParser.parseStatement(INT_I).asExpressionStmt().getExpression());
            forStmt.setInitialization(initialization);
            forStmt.setCompare(StaticJavaParser.parseExpression("i < batch.size() && i < keys.size()"));
            NodeList<Expression> update = new NodeList<>();
            update.add(StaticJavaParser.parseExpression("i++"));
            forStmt.setUpdate(update);
            BlockStmt forBody = new BlockStmt();
            forBody.addStatement(StaticJavaParser.parseStatement("batch.get(i)" + pkSetter + "(keys.get(i));"));
            forStmt.setBody(forBody);
            innerTryBlock.addStatement(forStmt);

            innerTryBlock.addStatement(StaticJavaParser.parseStatement("if (keys.size() != batch.size()) LOGGER.warn(\"Mismatched generated keys count: expected {}, got {}\", batch.size(), keys.size());"));

            innerTryStmt.setTryBlock(innerTryBlock);
            tryBlock.addStatement(innerTryStmt);
        }

        tryBlock.addStatement(new ReturnStmt(new NameExpr("results")));
        tryStmt.setTryBlock(tryBlock);

        // Add catch clause for processBatch (insert)
        CatchClause catchClause = new CatchClause();
        ClassOrInterfaceType sqlType = new ClassOrInterfaceType();
        sqlType.setName(SQL_EXCEPTION);
        catchClause.setParameter(new Parameter(sqlType,"e"));
        BlockStmt catchBlock = new BlockStmt();
        catchBlock.addStatement(StaticJavaParser.parseStatement(
                "throw new SQLException(String.format(\"Failed to batch insert %d items into table '%s' (pk: '%s'). See cause for details.\", batch.size(), TABLE, COL_" + pkField.toUpperCase() + STRING3
        ));
        catchClause.setBody(catchBlock);
        tryStmt.getCatchClauses().add(catchClause);



        block.addStatement(tryStmt);
        m.setBody(block);
    }

    private static void createFindByIdMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            String pkField,
            String idType) {

        MethodDeclaration m = daoClass.addMethod("findById", Modifier.Keyword.PUBLIC);
        m.setType(modelClassName);
        m.addParameter(CONNECTION, "conn");
        m.addParameter(mapJavaTypeToMethod(idType), "id");

        BlockStmt block = new BlockStmt();
        TryStmt tryStmt = new TryStmt();
        BlockStmt tryBlock = new BlockStmt();

        VariableDeclarator psDeclarator = new VariableDeclarator();
        psDeclarator.setName("ps");
        psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
        psDeclarator.setInitializer(StaticJavaParser.parseExpression("conn.prepareStatement(getSelectByColumnSql(COL_" + pkField.toUpperCase() + "))"));
        tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

        tryBlock.addStatement(StaticJavaParser.parseStatement("ps." + getPreparedStatementSetter(idType) + "(1, id);"));

        TryStmt innerTryStmt = new TryStmt();
        BlockStmt innerTryBlock = new BlockStmt();

        VariableDeclarator rsDeclarator = new VariableDeclarator();
        rsDeclarator.setName("rs");
        rsDeclarator.setType(new ClassOrInterfaceType(null, RESULT_SET));
        rsDeclarator.setInitializer(StaticJavaParser.parseExpression(PS_EXECUTE_QUERY));
        innerTryStmt.getResources().add(new VariableDeclarationExpr(rsDeclarator));

        innerTryBlock.addStatement(new ReturnStmt(
                StaticJavaParser.parseExpression("rs.next() ? extract(rs) : null")));
        innerTryStmt.setTryBlock(innerTryBlock);
        tryBlock.addStatement(innerTryStmt);

        tryStmt.setTryBlock(tryBlock);
        block.addStatement(tryStmt);

        m.setBody(block);
    }

    private static void createFindAllMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            ColumnProcessingResult columnResult) {

        String pkField = columnResult.pkInfo.field;
        String pkColConstant = "COL_" + pkField.toUpperCase();

        MethodDeclaration m = daoClass.addMethod("findAll", Modifier.Keyword.PUBLIC);
        // Use actual < and > characters (not HTML entities)
        m.setType("java.util.List<" + modelClassName + ">");
        m.addParameter(CONNECTION, "conn");
        m.addParameter("int", "page");
        m.addParameter("int", "pageSize");
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        BlockStmt block = new BlockStmt();

        // Input validation
        IfStmt pageCheck = new IfStmt();
        pageCheck.setCondition(StaticJavaParser.parseExpression("page < 1"));
        BlockStmt pageBlock = new BlockStmt();
        pageBlock.addStatement(new ThrowStmt(StaticJavaParser.parseExpression("new IllegalArgumentException(\"Page must be >= 1\")")));
        pageCheck.setThenStmt(pageBlock);
        block.addStatement(pageCheck);

        IfStmt sizeCheck = new IfStmt();
        sizeCheck.setCondition(StaticJavaParser.parseExpression("pageSize < 1"));
        BlockStmt sizeBlock = new BlockStmt();
        sizeBlock.addStatement(new ThrowStmt(StaticJavaParser.parseExpression("new IllegalArgumentException(\"Page size must be >= 1\")")));
        sizeCheck.setThenStmt(sizeBlock);
        block.addStatement(sizeCheck);

        // Build generalized SQL using String.format with constants
        String sqlFormat = "\"SELECT %s FROM %s ORDER BY %s OFFSET ? ROWS FETCH NEXT ? ROWS ONLY\"";
        block.addStatement(StaticJavaParser.parseStatement("String sql = String.format(" + sqlFormat + ", SELECT_COLUMNS, TABLE, " + pkColConstant + ");"));

        // Initialize result list
        block.addStatement(StaticJavaParser.parseStatement("java.util.List<" + modelClassName + "> list = new ArrayList<>();"));

        // Calculate offset
        block.addStatement(StaticJavaParser.parseStatement("int offset = (page - 1) * pageSize;"));

        // Try-with-resources for PreparedStatement
        TryStmt tryStmt = new TryStmt();
        BlockStmt tryBlock = new BlockStmt();

        VariableDeclarator psDeclarator = new VariableDeclarator();
        psDeclarator.setName("ps");
        psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
        psDeclarator.setInitializer(StaticJavaParser.parseExpression("conn.prepareStatement(sql)"));
        tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

        // Set pagination parameters directly
        tryBlock.addStatement(StaticJavaParser.parseStatement("ps.setInt(1, offset);"));
        tryBlock.addStatement(StaticJavaParser.parseStatement("ps.setInt(2, pageSize);"));

        // Inner try-with-resources for ResultSet
        TryStmt innerTryStmt = new TryStmt();
        BlockStmt innerTryBlock = new BlockStmt();

        VariableDeclarator rsDeclarator = new VariableDeclarator();
        rsDeclarator.setName("rs");
        rsDeclarator.setType(new ClassOrInterfaceType(null, RESULT_SET));
        rsDeclarator.setInitializer(StaticJavaParser.parseExpression(PS_EXECUTE_QUERY));
        innerTryStmt.getResources().add(new VariableDeclarationExpr(rsDeclarator));

        // Process results
        WhileStmt whileStmt = new WhileStmt();
        whileStmt.setCondition(StaticJavaParser.parseExpression(RS_NEXT));
        BlockStmt whileBlock = new BlockStmt();
        whileBlock.addStatement(StaticJavaParser.parseStatement("list.add(extract(rs));"));
        whileStmt.setBody(whileBlock);
        innerTryBlock.addStatement(whileStmt);

        innerTryStmt.setTryBlock(innerTryBlock);
        tryBlock.addStatement(innerTryStmt);
        tryStmt.setTryBlock(tryBlock);
        block.addStatement(tryStmt);

        block.addStatement(new ReturnStmt(new NameExpr("list")));
        m.setBody(block);
    }



    private static void createRelationshipMethods(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            List<ColumnInfo> columnsInfo,
            List<Relationship> relationships) {

        if (relationships == null) return;

        for (Relationship rel : relationships) {
            if (rel.getType() == Relationship.Type.MANY_TO_ONE) {
                String fkColumn = rel.getFkColumn();
                ColumnInfo fkColInfo = columnsInfo.stream().filter(c -> c.name.equals(fkColumn)).findFirst().orElse(null);
                if (fkColInfo == null) continue;
                String fkType = fkColInfo.javaType;
                String methodName = "findBy" + StringUtils.toCamelCase(fkColumn, NamingStrategyConfig.getUppercaseAcronyms(), true);
                String paramFkName = StringUtils.toCamelCase(fkColumn, NamingStrategyConfig.getUppercaseAcronyms(), false);

                MethodDeclaration m = daoClass.addMethod(methodName, Modifier.Keyword.PUBLIC);
                m.setType(LIST_GREATER + modelClassName + ">");
                m.addParameter(CONNECTION, "conn");
                m.addParameter(mapJavaTypeToMethod(fkType), paramFkName);
                m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

                BlockStmt block = new BlockStmt();
                block.addStatement(StaticJavaParser.parseStatement(LIST_GREATER + modelClassName + "> list = new ArrayList<>();"));

                TryStmt tryStmt = new TryStmt();
                BlockStmt tryBlock = new BlockStmt();

                VariableDeclarator psDeclarator = new VariableDeclarator();
                psDeclarator.setName("ps");
                psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
                psDeclarator.setInitializer(StaticJavaParser.parseExpression("conn.prepareStatement(getSelectByColumnSql(COL_" + fkColumn.toUpperCase() + "))"));
                tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

                tryBlock.addStatement(StaticJavaParser.parseStatement(
                        "ps." + getPreparedStatementSetter(fkType) + "(1, " + paramFkName + ");"));

                TryStmt innerTryStmt = new TryStmt();
                BlockStmt innerTryBlock = new BlockStmt();

                VariableDeclarator rsDeclarator = new VariableDeclarator();
                rsDeclarator.setName("rs");
                rsDeclarator.setType(new ClassOrInterfaceType(null, RESULT_SET));
                rsDeclarator.setInitializer(StaticJavaParser.parseExpression(PS_EXECUTE_QUERY));
                innerTryStmt.getResources().add(new VariableDeclarationExpr(rsDeclarator));

                WhileStmt whileStmt = new WhileStmt();
                whileStmt.setCondition(StaticJavaParser.parseExpression(RS_NEXT));
                BlockStmt whileBlock = new BlockStmt();
                whileBlock.addStatement(StaticJavaParser.parseStatement("list.add(extract(rs));"));
                whileStmt.setBody(whileBlock);
                innerTryBlock.addStatement(whileStmt);

                innerTryStmt.setTryBlock(innerTryBlock);
                tryBlock.addStatement(innerTryStmt);

                tryStmt.setTryBlock(tryBlock);
                block.addStatement(tryStmt);

                block.addStatement(new ReturnStmt(new NameExpr("list")));
                m.setBody(block);
            }
        }
    }

    private static void createUpdateMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            String pkField,
            String idType,
            List<ColumnInfo> columnsInfo) {

        MethodDeclaration m = daoClass.addMethod("update", Modifier.Keyword.PUBLIC);
        m.setType(BOOL);
        m.addParameter(CONNECTION, "conn");
        m.addParameter(modelClassName, StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false));
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        List<ColumnInfo> updateCols = columnsInfo.stream()
                .filter(c -> !c.name.equalsIgnoreCase(pkField))
                .collect(Collectors.toList());

        String paramName = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false);
        String className = modelClassName.replace("DTO", "");
        String pkGetter = paramName + ".get" + StringUtils.toCamelCase(pkField, NamingStrategyConfig.getUppercaseAcronyms(), true) + "()";

        BlockStmt block = new BlockStmt();
        block.addStatement(StaticJavaParser.parseStatement("if (" + pkGetter + " == null) throw new IllegalArgumentException(\"Primary key cannot be null for update\");"));

        TryStmt tryStmt = new TryStmt();
        BlockStmt tryBlock = new BlockStmt();

        VariableDeclarator psDeclarator = new VariableDeclarator();
        psDeclarator.setName("ps");
        psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
        psDeclarator.setInitializer(StaticJavaParser.parseExpression("conn.prepareStatement(UPDATE_SQL)"));
        tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

        tryBlock.addStatement(StaticJavaParser.parseStatement("set" + className + PARAMS + paramName + ");"));
        tryBlock.addStatement(StaticJavaParser.parseStatement(
                "ps." + getPreparedStatementSetter(idType) + "(" + (updateCols.size() + 1) + ", " + pkGetter + ");"));

        tryBlock.addStatement(new ReturnStmt(
                StaticJavaParser.parseExpression("ps.executeUpdate() > 0")));
        tryStmt.setTryBlock(tryBlock);
        block.addStatement(tryStmt);

        m.setBody(block);
    }

    private static void createBulkUpdateMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName) {

        MethodDeclaration m = daoClass.addMethod("updateAll", Modifier.Keyword.PUBLIC);
        m.setType(INT_ARRAY);
        m.addParameter(CONNECTION, "conn");
        m.addParameter(LIST_GREATER + modelClassName + ">", StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false) + "s");
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        String paramName = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false) + "s";
        String validationMethodName = "isInvalidUpdate" + modelClassName + "List";
        String processBatchMethodName = "processUpdateBatch";

        BlockStmt block = new BlockStmt();

        // Input validation using helper method
        block.addStatement(new IfStmt(
                StaticJavaParser.parseExpression(validationMethodName + "(" + paramName + ")"),
                new BlockStmt().addStatement(new ReturnStmt(StaticJavaParser.parseExpression("new int[0]"))),
                null
        ));

        // Setup batch processing
        block.addStatement(StaticJavaParser.parseStatement("int batchSize = UPDATE_BATCH_SIZE;"));
        block.addStatement(StaticJavaParser.parseStatement(LIST_LIST + modelClassName + ">> batches = chunkList(" + paramName + ", batchSize);"));
        block.addStatement(StaticJavaParser.parseStatement("int[] totalResults = new int[" + paramName + ".size()];"));
        block.addStatement(StaticJavaParser.parseStatement("int resultIndex = 0;"));

        // Transaction management
        block.addStatement(StaticJavaParser.parseStatement(AUTO_COMMIT));
        TryStmt outerTry = new TryStmt();
        BlockStmt outerTryBlock = new BlockStmt();
        outerTryBlock.addStatement(StaticJavaParser.parseStatement(CONN_SET_AUTO_COMMIT_FALSE));

        // Process batches using helper method
        ForEachStmt batchLoop = new ForEachStmt();
        batchLoop.setVariable(new VariableDeclarationExpr(new ClassOrInterfaceType(null, LIST_GREATER + modelClassName + ">"), BATCH));
        batchLoop.setIterable(new NameExpr("batches"));
        BlockStmt batchBody = new BlockStmt();
        batchBody.addStatement(StaticJavaParser.parseStatement("int[] results = " + processBatchMethodName + "(conn, batch);"));
        batchBody.addStatement(StaticJavaParser.parseStatement("System.arraycopy(results, 0, totalResults, resultIndex, results.length);"));
        batchBody.addStatement(StaticJavaParser.parseStatement("resultIndex += results.length;"));
        batchLoop.setBody(batchBody);
        outerTryBlock.addStatement(batchLoop);

        outerTryBlock.addStatement(StaticJavaParser.parseStatement(COMMIT));
        outerTry.setTryBlock(outerTryBlock);

        // Error handling
        CatchClause outerCatch = new CatchClause();
        ClassOrInterfaceType sqlType = new ClassOrInterfaceType();
        sqlType.setName(SQL_EXCEPTION);
        outerCatch.setParameter(new Parameter(sqlType,"e"));
        BlockStmt outerCatchBlock = new BlockStmt();
        outerCatchBlock.addStatement(StaticJavaParser.parseStatement(ROLL_BACK));
        // Rethrow with new contextual message
        outerCatchBlock.addStatement(StaticJavaParser.parseStatement("throw new SQLException(\"Failed to update all " + paramName + " in batch. Transaction rolled back.\", e);"));
        outerCatch.setBody(outerCatchBlock);
        outerTry.getCatchClauses().add(outerCatch);

        // Finally block
        BlockStmt finallyBlock = new BlockStmt();
        finallyBlock.addStatement(StaticJavaParser.parseStatement(CONN_SET_AUTO_COMMIT_AUTO_COMMIT));
        outerTry.setFinallyBlock(finallyBlock);

        block.addStatement(outerTry);
        block.addStatement(StaticJavaParser.parseStatement("return totalResults;"));
        m.setBody(block);
    }


    private static void createIsInvalidUpdateListMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            String pkField) {

        String methodName = "isInvalidUpdate" + modelClassName + "List";
        String paramName = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false) + "s";
        String itemVar = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false);
        String pkGetter = itemVar + ".get" + StringUtils.toCamelCase(pkField, NamingStrategyConfig.getUppercaseAcronyms(), true) + "()";

        MethodDeclaration m = daoClass.addMethod(methodName, Modifier.Keyword.PRIVATE);
        m.setType(BOOL);
        m.addParameter(LIST_GREATER + modelClassName + ">", paramName);

        BlockStmt block = new BlockStmt();

        // Check for null or empty list
        block.addStatement(new IfStmt(
                StaticJavaParser.parseExpression(paramName + " == null || " + paramName + ".isEmpty()"),
                new BlockStmt().addStatement(new ReturnStmt(StaticJavaParser.parseExpression("true"))),
                null
        ));

        // Check for null elements and null primary keys
        ForEachStmt checkLoop = new ForEachStmt();
        checkLoop.setVariable(new VariableDeclarationExpr(new ClassOrInterfaceType(null, modelClassName), itemVar));
        checkLoop.setIterable(new NameExpr(paramName));
        BlockStmt checkBody = new BlockStmt();
        checkBody.addStatement(new IfStmt(
                StaticJavaParser.parseExpression(itemVar + " == null"),
                new ThrowStmt(StaticJavaParser.parseExpression("new IllegalArgumentException(\"Null DTO in batch update\")")),
                null
        ));
        checkBody.addStatement(new IfStmt(
                StaticJavaParser.parseExpression(pkGetter + " == null"),
                new ThrowStmt(StaticJavaParser.parseExpression("new IllegalArgumentException(\"Null primary key in batch update\")")),
                null
        ));
        checkLoop.setBody(checkBody);
        block.addStatement(checkLoop);

        // Return false if validation passes
        block.addStatement(new ReturnStmt(StaticJavaParser.parseExpression(FALSE)));
        m.setBody(block);
    }

    private static void createProcessUpdateBatchMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            String pkField,
            String idType,
            List<ColumnInfo> columnsInfo) {

        String methodName = "processUpdateBatch";
        String className = modelClassName.replace("DTO", "");
        String itemVar = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false);
        String pkGetter = itemVar + ".get" + StringUtils.toCamelCase(pkField, NamingStrategyConfig.getUppercaseAcronyms(), true) + "()";

        List<ColumnInfo> updateCols = columnsInfo.stream()
                .filter(c -> !c.name.equalsIgnoreCase(pkField))
                .collect(Collectors.toList());

        MethodDeclaration m = daoClass.addMethod(methodName, Modifier.Keyword.PRIVATE);
        m.setType(INT_ARRAY);
        m.addParameter(CONNECTION, "conn");
        m.addParameter(LIST_GREATER + modelClassName + ">", BATCH);
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        BlockStmt block = new BlockStmt();

        TryStmt tryStmt = new TryStmt();
        BlockStmt tryBlock = new BlockStmt();

        // Create PreparedStatement
        VariableDeclarator psDeclarator = new VariableDeclarator();
        psDeclarator.setName("ps");
        psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
        psDeclarator.setInitializer(StaticJavaParser.parseExpression("conn.prepareStatement(UPDATE_SQL)"));
        tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

        // Add batch parameters
        ForEachStmt innerLoop = new ForEachStmt();
        innerLoop.setVariable(new VariableDeclarationExpr(new ClassOrInterfaceType(null, modelClassName), itemVar));
        innerLoop.setIterable(new NameExpr(BATCH));
        BlockStmt innerBody = new BlockStmt();
        innerBody.addStatement(StaticJavaParser.parseStatement("set" + className + PARAMS + itemVar + ");"));
        innerBody.addStatement(StaticJavaParser.parseStatement(
                "ps." + getPreparedStatementSetter(idType) + "(" + (updateCols.size() + 1) + ", " + pkGetter + ");"));
        innerBody.addStatement(StaticJavaParser.parseStatement("ps.addBatch();"));
        innerLoop.setBody(innerBody);
        tryBlock.addStatement(innerLoop);

        // Execute batch
        tryBlock.addStatement(StaticJavaParser.parseStatement("int[] results = ps.executeBatch();"));
        tryBlock.addStatement(StaticJavaParser.parseStatement("LOGGER.debug(\"Updated {} rows in batch\", results.length);"));
        tryBlock.addStatement(new ReturnStmt(new NameExpr("results")));

        tryStmt.setTryBlock(tryBlock);

        // Add catch clause for processUpdateBatch
        CatchClause catchClause = new CatchClause();
        ClassOrInterfaceType sqlType = new ClassOrInterfaceType();
        sqlType.setName(SQL_EXCEPTION);
        catchClause.setParameter(new Parameter(sqlType,"e"));
        BlockStmt catchBlock = new BlockStmt();
        catchBlock.addStatement(StaticJavaParser.parseStatement(
                "throw new SQLException(String.format(\"Failed to batch update %d items in table '%s' (pk: '%s'). See cause for details.\", batch.size(), TABLE, COL_" + pkField.toUpperCase() + STRING3
        ));
        catchClause.setBody(catchBlock);
        tryStmt.getCatchClauses().add(catchClause);

        block.addStatement(tryStmt);
        m.setBody(block);
    }

    private static void createDeleteMethod(
            ClassOrInterfaceDeclaration daoClass,
            String idType) {

        MethodDeclaration m = daoClass.addMethod("deleteById", Modifier.Keyword.PUBLIC);
        m.setType(BOOL);
        m.addParameter(CONNECTION, "conn");
        m.addParameter(mapJavaTypeToMethod(idType), "id");
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        BlockStmt block = new BlockStmt();
        TryStmt tryStmt = new TryStmt();
        BlockStmt tryBlock = new BlockStmt();

        VariableDeclarator psDeclarator = new VariableDeclarator();
        psDeclarator.setName("ps");
        psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
        psDeclarator.setInitializer(StaticJavaParser.parseExpression("conn.prepareStatement(DELETE_SQL)"));
        tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

        tryBlock.addStatement(StaticJavaParser.parseStatement("ps." + getPreparedStatementSetter(idType) + "(1, id);"));
        tryBlock.addStatement(new ReturnStmt(
                StaticJavaParser.parseExpression("ps.executeUpdate() > 0")));

        tryStmt.setTryBlock(tryBlock);
        block.addStatement(tryStmt);

        m.setBody(block);
    }

    private static void createBulkDeleteMethod(
            ClassOrInterfaceDeclaration daoClass,
            String idType) {


        MethodDeclaration m = daoClass.addMethod("deleteAllByIds", Modifier.Keyword.PUBLIC);
        m.setType("int");
        m.addParameter(CONNECTION, "conn");
        m.addParameter(LIST_GREATER + idType + ">", "ids");
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        String validationMethodName = "isInvalidIdsList";
        String processChunkMethodName = "processDeleteChunk";

        BlockStmt block = new BlockStmt();

        // Input validation using helper method
        block.addStatement(new IfStmt(
                StaticJavaParser.parseExpression(validationMethodName + "(ids)"),
                new BlockStmt().addStatement(new ReturnStmt(StaticJavaParser.parseExpression("0"))),
                null
        ));

        // Setup chunk processing
        block.addStatement(StaticJavaParser.parseStatement("int chunkSize = DELETE_CHUNK_SIZE;"));
        block.addStatement(StaticJavaParser.parseStatement(LIST_LIST + idType + ">> chunks = chunkList(ids, chunkSize);"));
        block.addStatement(StaticJavaParser.parseStatement("int totalDeleted = 0;"));

        // Transaction management
        block.addStatement(StaticJavaParser.parseStatement(AUTO_COMMIT));
        TryStmt outerTry = new TryStmt();
        BlockStmt outerTryBlock = new BlockStmt();
        outerTryBlock.addStatement(StaticJavaParser.parseStatement(CONN_SET_AUTO_COMMIT_FALSE));

        // Process chunks using helper method
        ForEachStmt chunkLoop = new ForEachStmt();
        chunkLoop.setVariable(new VariableDeclarationExpr(new ClassOrInterfaceType(null, LIST_GREATER + idType + ">"), "chunk"));
        chunkLoop.setIterable(new NameExpr("chunks"));
        BlockStmt chunkBody = new BlockStmt();
        chunkBody.addStatement(StaticJavaParser.parseStatement("int affected = " + processChunkMethodName + "(conn, chunk);"));
        chunkBody.addStatement(StaticJavaParser.parseStatement("totalDeleted += affected;"));
        chunkLoop.setBody(chunkBody);
        outerTryBlock.addStatement(chunkLoop);

        outerTryBlock.addStatement(StaticJavaParser.parseStatement(COMMIT));
        outerTry.setTryBlock(outerTryBlock);

        // Error handling
        CatchClause outerCatch = new CatchClause();
        ClassOrInterfaceType sqlType = new ClassOrInterfaceType();
        sqlType.setName(SQL_EXCEPTION);
        outerCatch.setParameter(new Parameter(sqlType,"e"));
        BlockStmt outerCatchBlock = new BlockStmt();
        outerCatchBlock.addStatement(StaticJavaParser.parseStatement(ROLL_BACK));
        // Rethrow with new contextual message
        outerCatchBlock.addStatement(StaticJavaParser.parseStatement(
                "throw new SQLException(\"Failed to delete all ids in batch. Transaction rolled back.\", e);"
        ));

        outerCatch.setBody(outerCatchBlock);
        outerTry.getCatchClauses().add(outerCatch);

        // Finally block
        BlockStmt finallyBlock = new BlockStmt();
        finallyBlock.addStatement(StaticJavaParser.parseStatement(CONN_SET_AUTO_COMMIT_AUTO_COMMIT));
        outerTry.setFinallyBlock(finallyBlock);

        block.addStatement(outerTry);
        block.addStatement(StaticJavaParser.parseStatement("return totalDeleted;"));
        m.setBody(block);
    }

    private static void createIsInvalidIdsListMethod(
            ClassOrInterfaceDeclaration daoClass,
            String idType) {

        String methodName = "isInvalidIdsList";

        MethodDeclaration m = daoClass.addMethod(methodName, Modifier.Keyword.PRIVATE);
        m.setType(BOOL);
        m.addParameter(LIST_GREATER + idType + ">", "ids");

        BlockStmt block = new BlockStmt();

        // Check for null or empty list
        block.addStatement(new IfStmt(
                StaticJavaParser.parseExpression("ids == null || ids.isEmpty()"),
                new BlockStmt().addStatement(new ReturnStmt(StaticJavaParser.parseExpression(TRUE))),
                null
        ));

        // Check for null IDs
        ForEachStmt checkLoop = new ForEachStmt();
        checkLoop.setVariable(new VariableDeclarationExpr(new ClassOrInterfaceType(null, idType), "id"));
        checkLoop.setIterable(new NameExpr("ids"));
        BlockStmt checkBody = new BlockStmt();
        checkBody.addStatement(new IfStmt(
                StaticJavaParser.parseExpression("id == null"),
                new ThrowStmt(StaticJavaParser.parseExpression("new IllegalArgumentException(\"Null ID in batch delete\")")),
                null
        ));
        checkLoop.setBody(checkBody);
        block.addStatement(checkLoop);

        // Return false if validation passes
        block.addStatement(new ReturnStmt(StaticJavaParser.parseExpression(FALSE)));
        m.setBody(block);
    }

    private static void createProcessDeleteChunkMethod(
            ClassOrInterfaceDeclaration daoClass,
            String pkField,
            String idType) {

        String methodName = "processDeleteChunk";

        MethodDeclaration m = daoClass.addMethod(methodName, Modifier.Keyword.PRIVATE);
        m.setType("int");
        m.addParameter(CONNECTION, "conn");
        m.addParameter(LIST_GREATER + idType + ">", "chunk");
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        BlockStmt block = new BlockStmt();

        // Build dynamic SQL with placeholders
        block.addStatement(StaticJavaParser.parseStatement("String placeholders = String.join(\", \", java.util.Collections.nCopies(chunk.size(), \"?\"));"));
        block.addStatement(StaticJavaParser.parseStatement("String sql = String.format(\"DELETE FROM %s WHERE %s IN (%s)\", TABLE, COL_" + pkField.toUpperCase() + ", placeholders);"));

        TryStmt tryStmt = new TryStmt();
        BlockStmt tryBlock = new BlockStmt();

        // Create PreparedStatement
        VariableDeclarator psDeclarator = new VariableDeclarator();
        psDeclarator.setName("ps");
        psDeclarator.setType(new ClassOrInterfaceType(null, PREPARED_STATEMENT));
        psDeclarator.setInitializer(StaticJavaParser.parseExpression("conn.prepareStatement(sql)"));
        tryStmt.getResources().add(new VariableDeclarationExpr(psDeclarator));

        // Set parameters
        ForStmt setLoop = new ForStmt();
        NodeList<Expression> init = new NodeList<>();
        init.add(StaticJavaParser.parseStatement(INT_I).asExpressionStmt().getExpression());
        setLoop.setInitialization(init);
        setLoop.setCompare(StaticJavaParser.parseExpression("i < chunk.size()"));
        NodeList<Expression> update = new NodeList<>();
        update.add(StaticJavaParser.parseExpression("i++"));
        setLoop.setUpdate(update);
        BlockStmt setBody = new BlockStmt();
        setBody.addStatement(StaticJavaParser.parseStatement("ps." + getPreparedStatementSetter(idType) + "(i + 1, chunk.get(i));"));
        setLoop.setBody(setBody);
        tryBlock.addStatement(setLoop);

        // Execute and return result
        tryBlock.addStatement(StaticJavaParser.parseStatement("int affected = ps.executeUpdate();"));
        tryBlock.addStatement(StaticJavaParser.parseStatement("LOGGER.debug(\"Deleted {} rows in batch\", affected);"));
        tryBlock.addStatement(new ReturnStmt(new NameExpr("affected")));

        tryStmt.setTryBlock(tryBlock);

        // Add catch clause
        CatchClause catchClause = new CatchClause();
        ClassOrInterfaceType sqlType = new ClassOrInterfaceType();
        sqlType.setName(SQL_EXCEPTION);
        catchClause.setParameter(new Parameter(sqlType, "e"));
        BlockStmt catchBlock = new BlockStmt();
        catchBlock.addStatement(StaticJavaParser.parseStatement(
                "throw new SQLException(String.format(\"Failed to batch delete %d items from table '%s' (pk: '%s'). See cause for details.\", chunk.size(), TABLE, COL_" + pkField.toUpperCase() + STRING3
        ));
        catchClause.setBody(catchBlock);
        tryStmt.getCatchClauses().add(catchClause);



        block.addStatement(tryStmt);
        m.setBody(block);
    }

    private static void createSetParamsMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            List<ColumnInfo> columnsInfo,
            String pkField,
            boolean pkIsAuto) {

        // Now proceed with creating the setParams method
        String className = modelClassName.replace("DTO", "");
        MethodDeclaration m = daoClass.addMethod("set" + className + "Params", Modifier.Keyword.PRIVATE);
        m.setType("void");
        m.addParameter(PREPARED_STATEMENT, "ps");
        m.addParameter(modelClassName, StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false));
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        List<ColumnInfo> paramsCols = columnsInfo.stream()
                .filter(c -> !(pkIsAuto && c.name.equals(pkField)))
                .toList();

        String paramName = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false);

        BlockStmt block = new BlockStmt();

        for (int i = 0; i < paramsCols.size(); i++) {
            ColumnInfo c = paramsCols.get(i);
            String getter = paramName + ".get" + StringUtils.toCamelCase(c.name, NamingStrategyConfig.getUppercaseAcronyms(), true) + "()";
            int idx = i + 1;

            String typesConstant = getTypesConstant(c.javaType);
            String valueExpr = getter;

            if (c.isForeignKey) {
                String baseName = c.name.toLowerCase().endsWith("_id") ? c.name.substring(0, c.name.length() - 3) : c.name;
                String relationshipName = StringUtils.toCamelCase(baseName, NamingStrategyConfig.getUppercaseAcronyms(), true);
                String relationGetter = paramName + ".get" + relationshipName + "()";
                String relatedPk = c.referencedTable + "_id";
                String relatedPkCamel = StringUtils.toCamelCase(relatedPk, NamingStrategyConfig.getUppercaseAcronyms(), true);
                valueExpr = relationGetter + " != null ? " + relationGetter + ".get" + relatedPkCamel + "() : null";
            } else if (LOCAL_DATE_TIME.equals(c.javaType)) {
                valueExpr = "toTimestamp(" + getter + ")";
            } else if (LOCAL_DATE.equals(c.javaType)) {
                valueExpr = getter + " != null ? java.sql.Date.valueOf(" + getter + BRACE_NULL;
            } else if (LOCAL_TIME.equals(c.javaType)) {
                valueExpr = getter + " != null ? java.sql.Time.valueOf(" + getter + BRACE_NULL;
            } else if (c.javaType.equals(STRING) && c.isBinary) {
                valueExpr = getter + " != null ? " + getter + ".getBytes() : null";
            }

            String setStmt = "setNullable(ps, " + idx + ", " + valueExpr + ", " + typesConstant + ");";
            block.addStatement(StaticJavaParser.parseStatement(setStmt));
        }
        m.setBody(block);
    }


    private static void createExtractMethod(
            ClassOrInterfaceDeclaration daoClass,
            String modelClassName,
            List<ColumnInfo> columnsInfo) {

        MethodDeclaration m = daoClass.addMethod("extract", Modifier.Keyword.PRIVATE);
        m.setType(modelClassName);
        m.addParameter(RESULT_SET, "rs");
        m.addThrownException(new ClassOrInterfaceType(null, SQL_EXCEPTION));

        String objectName = StringUtils.toCamelCase(modelClassName, NamingStrategyConfig.getUppercaseAcronyms(), false);

        BlockStmt block = new BlockStmt();
        block.addStatement(StaticJavaParser.parseStatement(
                modelClassName + " " + objectName + " = new " + modelClassName + "();"));

        for (ColumnInfo c : columnsInfo) {
            String colConst = "COL_" + c.name.toUpperCase();

            // --- Use camelCase for variable names ---
            String varName = StringUtils.toCamelCase(c.name, NamingStrategyConfig.getUppercaseAcronyms(), false);
            String setter = objectName + ".set" + StringUtils.toCamelCase(c.name, NamingStrategyConfig.getUppercaseAcronyms(), true);

            if (LOCAL_DATE_TIME.equals(c.javaType)) {
                block.addStatement(StaticJavaParser.parseStatement("Timestamp " + varName + " = rs.getTimestamp(" + colConst + ");"));
                block.addStatement(new IfStmt(
                        StaticJavaParser.parseExpression(varName + STRING2),
                        new ExpressionStmt(StaticJavaParser.parseExpression(setter + "(" + varName + ".toLocalDateTime())")),
                        null));
            } else if (LOCAL_DATE.equals(c.javaType)) {
                block.addStatement(StaticJavaParser.parseStatement("java.sql.Date " + varName + " = rs.getDate(" + colConst + ");"));
                block.addStatement(new IfStmt(
                        StaticJavaParser.parseExpression(varName + STRING2),
                        new ExpressionStmt(StaticJavaParser.parseExpression(setter + "(" + varName + ".toLocalDate())")),
                        null));
            } else if (LOCAL_TIME.equals(c.javaType)) {
                block.addStatement(StaticJavaParser.parseStatement("java.sql.Time " + varName + " = rs.getTime(" + colConst + ");"));
                block.addStatement(new IfStmt(
                        StaticJavaParser.parseExpression(varName + STRING2),
                        new ExpressionStmt(StaticJavaParser.parseExpression(setter + "(" + varName + ".toLocalTime())")),
                        null));
            } else if (c.isForeignKey) {
                String baseName = c.name.toLowerCase().endsWith("_id") ? c.name.substring(0, c.name.length() - 3) : c.name;
                String relationshipName = StringUtils.toCamelCase(baseName, NamingStrategyConfig.getUppercaseAcronyms(), true);
                String relatedObjectName = StringUtils.toCamelCase(baseName, NamingStrategyConfig.getUppercaseAcronyms(), false);
                String relatedClass = StringUtils.toCamelCase(c.referencedTable, NamingStrategyConfig.getUppercaseAcronyms(), true);
                String relatedPk = c.referencedTable + "_id";
                String relatedPkCamel = StringUtils.toCamelCase(relatedPk, NamingStrategyConfig.getUppercaseAcronyms(), true);
                block.addStatement(StaticJavaParser.parseStatement(
                        c.javaType + " " + varName + " = rs.getObject(" + colConst + ", " + c.javaType + ".class);"));
                block.addStatement(StaticJavaParser.parseStatement(setter + "(" + varName + ");"));
                BlockStmt ifBlock = new BlockStmt();
                ifBlock.addStatement(StaticJavaParser.parseStatement(
                        relatedClass + " " + relatedObjectName + " = new " + relatedClass + "();"));
                ifBlock.addStatement(StaticJavaParser.parseStatement(
                        relatedObjectName + ".set" + relatedPkCamel + "(" + varName + ");"));
                ifBlock.addStatement(StaticJavaParser.parseStatement(
                        objectName + ".set" + relationshipName + "(" + relatedObjectName + ");"));
                block.addStatement(new IfStmt(
                        StaticJavaParser.parseExpression(varName + STRING2),
                        ifBlock,
                        null));
            } else if (c.javaType.equals(STRING) && c.isBinary) {
                block.addStatement(StaticJavaParser.parseStatement("byte[] bytes = rs.getBytes(" + colConst + ");"));
                block.addStatement(StaticJavaParser.parseStatement(setter + "(bytes != null ? new String(bytes) : null);"));
            } else if (Arrays.asList(INT_TYPE, LONG, FLOAT, DOUBLE, BIG_DECIMAL_IMPORT, BOOLEAN).contains(c.javaType)) {
                block.addStatement(StaticJavaParser.parseStatement(
                        c.javaType + " " + varName + " = rs.getObject(" + colConst + ", " + c.javaType + ".class);"));
                block.addStatement(StaticJavaParser.parseStatement(setter + "(" + varName + ");"));
            } else {
                block.addStatement(StaticJavaParser.parseStatement(
                        setter + "(rs." + getResultSetGetter(c.javaType) + "(" + colConst + "));"));
            }
        }
        block.addStatement(new ReturnStmt(new NameExpr(objectName)));
        m.setBody(block);
    }


    private static void writeToFile(TargetConfig target, String packageName, String className, CompilationUnit cu) throws IOException {
        Path outputPath = Paths.get(target.getOutputDirectory(), packageName.replace(".", "/"), className + ".java");
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, cu.toString());
        log.info("Generated DAO: {}", outputPath);
    }

    /* ---------- utility methods ---------- */

    private static String mapDbTypeToJava(String dbType, String columnName, int columnSize, String vendor) {
        // Handle null inputs
        if (columnName == null) return STRING;
        if (dbType == null) return STRING;

        String colNameLower = columnName.toLowerCase();

        // Try column name-based mapping first
        String columnBasedType = getTypeFromColumnName(colNameLower);
        if (columnBasedType != null) {
            return columnBasedType;
        }

        // Fall back to database type mapping
        return getTypeFromDbType(dbType.toUpperCase(), colNameLower, columnSize, vendor);
    }

    private static String getTypeFromColumnName(String colNameLower) {
        // Handle address fields
        if (isAddressField(colNameLower)) {
            return STRING;
        }

        // Handle boolean fields
        if (isBooleanField(colNameLower)) {
            return BOOLEAN;
        }

        // Handle duration/numeric fields
        if (isDurationField(colNameLower)) {
            return INT_TYPE;
        }

        // Handle ID fields
        if (isIdField(colNameLower)) {
            return INT_TYPE;
        }

        // Handle monetary fields (but not date fields)
        if (isMonetaryField(colNameLower) && !isDateTimeField(colNameLower)) {
            return BIG_DECIMAL_IMPORT;
        }

        return null; // No column-based mapping found
    }

    private static final Map<String, String> DB_TYPE_TO_JAVA;

    static {
        DB_TYPE_TO_JAVA = Map.ofEntries(
                Map.entry("SERIAL", INT_TYPE),
                Map.entry("YEAR", INT_TYPE),
                Map.entry("INT", INT_TYPE),
                Map.entry("INTEGER", INT_TYPE),
                Map.entry("SMALLINT", INT_TYPE),
                Map.entry("INT4", INT_TYPE),
                Map.entry("INT2", INT_TYPE),
                Map.entry("VARCHAR", STRING),
                Map.entry("VARCHAR2", STRING),
                Map.entry("CHAR", STRING),
                Map.entry("TEXT", STRING),
                Map.entry("CLOB", STRING),
                Map.entry("LONGTEXT", STRING),
                Map.entry("MEDIUMTEXT", STRING),
                Map.entry("NVARCHAR2", STRING),
                Map.entry("NCLOB", STRING),
                Map.entry("LONG", STRING),
                Map.entry("BIGINT", LONG),
                Map.entry("INT8", LONG),
                Map.entry("DECIMAL", BIG_DECIMAL_IMPORT),
                Map.entry("NUMERIC", BIG_DECIMAL_IMPORT),
                Map.entry("NUMBER", BIG_DECIMAL_IMPORT),
                Map.entry("BOOLEAN", BOOLEAN),
                Map.entry("BOOL", BOOLEAN),
                Map.entry("BIT", BOOLEAN),
                Map.entry("DATE", LOCAL_DATE),
                Map.entry("TIME", LOCAL_DATE),
                Map.entry("TIMESTAMP", LOCAL_DATE_TIME),
                Map.entry("TIMESTAMPTZ", LOCAL_DATE_TIME),
                Map.entry("DATETIME", LOCAL_DATE_TIME),
                Map.entry("JSON", MAP),
                Map.entry("JSONB", MAP),
                Map.entry("ARRAY", LIST),
                Map.entry("BLOB", BYTE),
                Map.entry("BYTEA", BYTE),
                Map.entry("BINARY", BYTE),
                Map.entry("VARBINARY", BYTE),
                Map.entry("RAW", BYTE),
                Map.entry("UUID", UID)
        );
    }

    private static String getTypeFromDbType(String dbType, String colNameLower, int columnSize, String vendor) {
        // Vendor-specific adjustments
        if ("ORACLE".equals(vendor) && "DATE".equals(dbType)) {
            return LOCAL_DATE_TIME;
        }

        // Special cases
        if ("TINYINT".equals(dbType)) {
            return (columnSize == 1) ? BOOLEAN : INT_TYPE;
        }

        if (List.of("FLOAT", "REAL", "BINARY_FLOAT").contains(dbType)) {
            return getFloatType(colNameLower);
        }

        if (List.of("DOUBLE", "DOUBLE PRECISION", "BINARY_DOUBLE").contains(dbType)) {
            return getDoubleType(colNameLower);
        }

        // Lookup in map
        return DB_TYPE_TO_JAVA.getOrDefault(dbType, STRING);
    }

    private static boolean isAddressField(String colNameLower) {
        return colNameLower.equals("address") || colNameLower.equals("address2");
    }

    private static boolean isBooleanField(String colNameLower) {
        return colNameLower.equals("active") || colNameLower.equals("is_active") ||
                colNameLower.equals("enabled") || colNameLower.equals("is_enabled");
    }

    private static boolean isDurationField(String colNameLower) {
        return colNameLower.contains("duration") || colNameLower.contains("length") ||
                colNameLower.equals("release_year") || colNameLower.equals("year");
    }

    private static boolean isIdField(String colNameLower) {
        return colNameLower.equals("id") || colNameLower.endsWith("_id") || colNameLower.endsWith("_no");
    }

    private static boolean isMonetaryField(String colNameLower) {
        return colNameLower.contains("amount") || colNameLower.contains("price") ||
                colNameLower.contains("cost") || colNameLower.contains("rate") ||
                colNameLower.contains("fee") || colNameLower.contains("balance") ||
                colNameLower.contains("total") || colNameLower.equals("payment") ||
                colNameLower.contains("payment_amount") || colNameLower.contains("payment_total");
    }

    private static boolean isDateTimeField(String colNameLower) {
        return colNameLower.contains("date") || colNameLower.contains("time") ||
                colNameLower.contains("created") || colNameLower.contains("updated") ||
                colNameLower.contains("modified") || colNameLower.contains("timestamp");
    }

    private static String getFloatType(String colNameLower) {
        return isMonetaryField(colNameLower) ? BIG_DECIMAL_IMPORT : FLOAT;
    }

    private static String getDoubleType(String colNameLower) {
        return isMonetaryField(colNameLower) ? BIG_DECIMAL_IMPORT : DOUBLE;
    }

    private static String mapJavaTypeToMethod(String javaType) {
        return switch (javaType) {
            case INT_TYPE -> "int";
            case LONG -> "long";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case BOOLEAN -> BOOL;
            default -> javaType;
        };
    }

    private static String getResultSetGetter(String javaType) {
        return switch (javaType) {
            case STRING -> "getString";
            case INT_TYPE -> "getInt";
            case LONG -> "getLong";
            case FLOAT -> "getFloat";
            case DOUBLE -> "getDouble";
            case BOOLEAN -> "getBoolean";
            case LOCAL_DATE -> "getDate";
            case LOCAL_TIME -> "getTime";
            case LOCAL_DATE_TIME -> "getTimestamp";
            case BIG_DECIMAL_IMPORT -> "getBigDecimal";
            case BYTE -> "getBytes";
            case UID -> SQL_GET_OBJECT;
            case MAP -> SQL_GET_OBJECT;
            case LIST -> SQL_GET_OBJECT;
            default -> SQL_GET_OBJECT;
        };
    }

    private static String getPreparedStatementSetter(String javaType) {
        return switch (javaType) {
            case STRING -> "setString";
            case INT_TYPE -> "setInt";
            case LONG -> "setLong";
            case FLOAT -> "setFloat";
            case DOUBLE -> "setDouble";
            case BOOLEAN -> "setBoolean";
            case LOCAL_DATE -> "setDate";
            case LOCAL_TIME -> "setTime";
            case LOCAL_DATE_TIME -> "setTimestamp";
            case BIG_DECIMAL_IMPORT -> "setBigDecimal";
            case BYTE -> "setBytes";
            case UID -> SQL_OBJECT;
            case MAP -> SQL_OBJECT;
            case LIST -> SQL_OBJECT;
            default -> SQL_OBJECT;
        };
    }

    private static String getTypesConstant(String javaType) {
        return switch (javaType) {
            case INT_TYPE -> "Types.INTEGER";
            case LONG -> "Types.BIGINT";
            case FLOAT -> "Types.FLOAT";
            case DOUBLE -> "Types.DOUBLE";
            case BOOLEAN -> "Types.BOOLEAN";
            case LOCAL_DATE -> "Types.DATE";
            case LOCAL_TIME -> "Types.TIME";
            case LOCAL_DATE_TIME -> "Types.TIMESTAMP";
            case BIG_DECIMAL_IMPORT -> "Types.NUMERIC";
            case BYTE -> "Types.BINARY";
            case STRING -> "Types.VARCHAR";
            default -> "Types.OTHER";
        };
    }
}
