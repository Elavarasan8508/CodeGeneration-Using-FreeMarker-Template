package com.bsit.codegeneration.metadata;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;
import com.bsit.codegeneration.IPojoGenerator;
import com.bsit.codegeneration.PojoGeneratorFactory;
import com.bsit.codegeneration.model.DatabaseConfig;
import com.bsit.codegeneration.model.TargetConfig;
import com.bsit.codegeneration.model.RecordConfig;
import com.bsit.codegeneration.model.DaoConfig;
import com.bsit.codegeneration.model.RepositoryConfig;
import com.bsit.codegeneration.model.PojoConfig;
import com.bsit.codegeneration.parser.RecordGenerator;
import com.bsit.codegeneration.parser.JdbiDaoGenerator;
import com.bsit.codegeneration.parser.JdbcDaoGenerator;
import com.bsit.codegeneration.parser.RepositoryGenerator;
import com.bsit.codegeneration.util.Relationship;
import com.bsit.codegeneration.util.Relationship.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbReader {

    public static final int INT = 2;
    private static final Logger logger = LoggerFactory.getLogger(DbReader.class);
    private static final Logger filmLogger = LoggerFactory.getLogger("filmLogger"); // Custom logger for "film" table

    private DbReader() { }

    public static void readDatabase(DatabaseConfig dbConfig,
                                    TargetConfig target,
                                    RecordConfig recordConfig,
                                    DaoConfig daoConfig,
                                    RepositoryConfig repositoryConfig,
                                    PojoConfig pojoConfig) throws SQLException, ClassNotFoundException {  // Removed dtoConfig

        Class.forName(dbConfig.getDriver());

        try (Connection conn = DriverManager.getConnection(
                dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword())) {

            DatabaseMetaData metaData = conn.getMetaData();
            String schema = dbConfig.getSchema();
            List<String> tableTypes = dbConfig.getInputFilter().getTableTypes();

            ResultSet tables = metaData.getTables(null, schema, "%", tableTypes.toArray(new String[0]));
            Set<String> processedTables = new HashSet<>();
            int count = 0;
            Map<String, List<Relationship>> allRelationships = readRelationships(conn, schema);

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                if (processedTables.contains(tableName.toLowerCase()))
                    continue;
                processedTables.add(tableName.toLowerCase());

                if (!isIncluded(tableName, dbConfig.getIncludes(), dbConfig.getExcludes())) {
                    continue;
                }

                Logger currentLogger;
                if (tableName.equalsIgnoreCase("film")) {
                    currentLogger = filmLogger;
                } else {
                    currentLogger = logger;
                }
                currentLogger.info("Reading table: {}", tableName);
                count++;

                List<Relationship> relationships;
                if (pojoConfig.isIncludeRelationships()) {  // Use pojoConfig
                    relationships = allRelationships.getOrDefault(tableName, Collections.emptyList());
                } else {
                    relationships = Collections.emptyList();
                }
                List<Relationship> reverseRelationships;
                if (pojoConfig.isIncludeReverseRelationships()) {  // Use pojoConfig
                    reverseRelationships = getReverseRelationships(metaData, schema, tableName);
                } else {
                    reverseRelationships = Collections.emptyList();
                }

                processGenerators(tableName, metaData, schema, dbConfig, target, recordConfig,
                        daoConfig, repositoryConfig, relationships, reverseRelationships, pojoConfig, currentLogger, conn);  // Passed conn at the end
            }

            logger.info("Total processed tables: {}", count);
            tables.close();
        }
    }

    private static void processGenerators(String tableName, DatabaseMetaData metaData, String schema,
                                          DatabaseConfig dbConfig, TargetConfig target,
                                          RecordConfig recordConfig,
                                          DaoConfig daoConfig, RepositoryConfig repositoryConfig,
                                          List<Relationship> relationships, List<Relationship> reverseRelationships,
                                          PojoConfig pojoConfig,
                                          Logger currentLogger, Connection conn) throws SQLException {  // Added conn parameter

        // Replaced DTO generation with POJO (using factory for switching)
        if (pojoConfig.isGenerate()) {
            try (ResultSet columns = metaData.getColumns(null, schema, tableName, "%")) {
                String generatorType = pojoConfig.getGenerator();  // From YAML
                IPojoGenerator pojoGenerator = PojoGeneratorFactory.create(generatorType);
                pojoGenerator.generate(conn, schema, target.getBasePackage(), target.getOutputDirectory());
                currentLogger.info("POJO generated for: {} using {}", tableName, generatorType);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        if (recordConfig.isGenerate()) {
            try (ResultSet columns = metaData.getColumns(null, schema, tableName, "%")) {
                RecordGenerator.generateRecord(tableName, columns, dbConfig, target, recordConfig, relationships, reverseRelationships);
                currentLogger.info("Record generated for: {}", tableName);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        if (daoConfig.isGenerate()) {
            try (ResultSet columns = metaData.getColumns(null, schema, tableName, "%")) {
                JdbcDaoGenerator.generateDao(tableName, columns, dbConfig, target, relationships);
                currentLogger.info("DAO generated for: {}", tableName);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        if (daoConfig.isGenerate()) {
            try (ResultSet columns = metaData.getColumns(null, schema, tableName, "%")) {
                JdbiDaoGenerator.generateDao(tableName, columns, dbConfig, target, relationships);
                currentLogger.info("Jdbi DAO generated for: {}", tableName);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        if (repositoryConfig.isGenerate()) {
            try (ResultSet columns = metaData.getColumns(null, schema, tableName, "%")) {
                RepositoryGenerator.generateRepository(tableName, columns, dbConfig, target);
                currentLogger.info("Repository generated for: {}", tableName);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static boolean isIncluded(String tableName, List<String> includes, List<String> excludes) {
        for (String pattern : excludes) {
            if (tableName.matches(pattern))
                return false;
        }
        for (String pattern : includes) {
            if (tableName.matches(pattern))
                return true;
        }
        return false;
    }

    private static List<Relationship> getReverseRelationships(DatabaseMetaData metaData, String schema, String tableName) throws SQLException {
        List<Relationship> revRels = new ArrayList<>();
        try (ResultSet rs = metaData.getExportedKeys(null, schema, tableName)) {
            while (rs.next()) {
                revRels.add(new Relationship(
                        rs.getString("FKTABLE_NAME"),
                        rs.getString("PKCOLUMN_NAME"),
                        rs.getString("FKCOLUMN_NAME"),
                        true,
                        Relationship.Type.ONE_TO_MANY,
                        null
                ));
            }
        }
        return revRels;
    }

    public static Map<String, List<Relationship>> readRelationships(Connection connection, String schema) throws SQLException {
        Map<String, List<Relationship>> relationshipMap = new HashMap<>();
        DatabaseMetaData metaData = connection.getMetaData();

        // Collect table names first
        List<String> tableNames = new ArrayList<>();
        try (ResultSet tables = metaData.getTables(null, schema, null, new String[]{"TABLE"})) {
            while (tables.next()) {
                tableNames.add(tables.getString("TABLE_NAME"));
            }
        }

        // Process each table
        Map<String, List<Relationship>> fkRels = new HashMap<>();
        for (String tableName : tableNames) {
            try (ResultSet fks = metaData.getImportedKeys(null, schema, tableName)) {
                List<Relationship> currentRels = new ArrayList<>();
                while (fks.next()) {
                    String pkTable = fks.getString("PKTABLE_NAME");
                    String fkColumn = fks.getString("FKCOLUMN_NAME");
                    String pkColumn = fks.getString("PKCOLUMN_NAME");
                    Relationship rel = new Relationship(pkTable, pkColumn, fkColumn, false, Type.MANY_TO_ONE, null);
                    currentRels.add(rel);

                    relationshipMap.computeIfAbsent(pkTable, k -> new ArrayList<>())
                            .add(new Relationship(tableName, pkColumn, fkColumn, true, Type.ONE_TO_MANY, null));
                }
                if (!currentRels.isEmpty()) {
                    fkRels.put(tableName, currentRels);
                }
                relationshipMap.put(tableName, currentRels);
            }
        }

        // Detect MANY_TO_MANY join tables
        for (Map.Entry<String, List<Relationship>> entry : fkRels.entrySet()) {
            String joinTable = entry.getKey();
            List<Relationship> rels = entry.getValue();

            if (rels.size() == INT) {
                Relationship r1 = rels.get(0);
                Relationship r2 = rels.get(1);

                relationshipMap.computeIfAbsent(r1.getRelatedTable(), k -> new ArrayList<>())
                        .add(new Relationship(r2.getRelatedTable(), r2.getPkColumn(), r2.getFkColumn(), false, Type.MANY_TO_MANY, joinTable));
                relationshipMap.computeIfAbsent(r2.getRelatedTable(), k -> new ArrayList<>())
                        .add(new Relationship(r1.getRelatedTable(), r1.getPkColumn(), r1.getFkColumn(), false, Type.MANY_TO_MANY, joinTable));
            }
        }

        return relationshipMap;
    }
}
