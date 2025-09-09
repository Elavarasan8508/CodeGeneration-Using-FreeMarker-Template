package com.bsit.codegeneration.metadata;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;

import com.bsit.codegeneration.model.CustomGeneratorConstructor;
import com.bsit.codegeneration.model.GeneratorConfig;
import com.bsit.codegeneration.model.GeneratorSettings;
import com.bsit.codegeneration.model.RepositoryConfig;
import com.bsit.codegeneration.model.DatabaseConfig;
import com.bsit.codegeneration.model.TargetConfig;
import com.bsit.codegeneration.model.PojoConfig;  // Updated to focus on PojoConfig
import com.bsit.codegeneration.model.RecordConfig;
import com.bsit.codegeneration.model.DaoConfig;

import org.yaml.snakeyaml.Yaml;

import static com.bsit.codegeneration.parser.JdbcDaoGenerator.log;

public class YamlParser {

    public void generate() throws Exception {
        Yaml yaml = new Yaml(new CustomGeneratorConstructor());

        try (InputStream input = Files.newInputStream(Paths.get("src/main/resources/generator.yml"))) {
            GeneratorConfig config = yaml.loadAs(input, GeneratorConfig.class);
            GeneratorSettings generator = config.getGenerator();

            DatabaseConfig dbConfig = generator.getDatabase();
            TargetConfig targetConfig = generator.getTarget();
            PojoConfig pojoConfig = generator.getPojo();
            RecordConfig recordConfig = generator.getRecord();
            DaoConfig daoConfig = generator.getDao();
            RepositoryConfig repositoryConfig = generator.getRepository();

            // Set up DB connection using details from YAML
            Class.forName(dbConfig.getDriver());
            try (Connection conn = DriverManager.getConnection(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword())) {


                // Proceed with other generation (e.g., Record, DAO) - no DTO
                DbReader.readDatabase(dbConfig, targetConfig, recordConfig, daoConfig, repositoryConfig,pojoConfig);  // Removed dtoConfig
            }

            log.info("Code generation completed successfully");
        }
    }
}
