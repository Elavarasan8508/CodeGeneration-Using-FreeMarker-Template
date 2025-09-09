package com.bsit.codegeneration.model;

import java.util.List;

public class DatabaseConfig {
    private static final String POSTGRES = "POSTGRES";
    private static final String POSTGRES_SQL = "POSTGRESQL";
    private static final String MYSQL = "MYSQL";
    private static final String ORACLE = "ORACLE";
    
    private String name;
    private String dialect;
    private String driver;
    private String url;
    private String user;
    private String password;
    private String schema;
    private List<String> includes;
    private List<String> excludes;
    private InputFilterConfig inputFilter;
    private NamingStrategyConfig namingStrategy;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDialect() {
        return dialect;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public String getDatabaseVendor() {
        if (dialect == null) {
            return "";
        }

        String vendor = extractVendorFromDialect(dialect.toUpperCase());
        if (!vendor.isEmpty()) {
            return vendor;
        }

        vendor = extractVendorFromUrl();
        if (!vendor.isEmpty()) {
            return vendor;
        }

        vendor = extractVendorFromDriver();
        return vendor.isEmpty() ? dialect.toUpperCase() : vendor;
    }

    private String extractVendorFromDialect(String dialectUpper) {
        if (dialectUpper.contains(POSTGRES)) return POSTGRES_SQL;
        if (dialectUpper.contains(MYSQL)) return MYSQL;
        if (dialectUpper.contains(ORACLE)) return ORACLE;
        if (dialectUpper.contains("H2")) return "H2";
        if (dialectUpper.contains("MSSQL") || dialectUpper.contains("SQL SERVER") || dialectUpper.contains("SQLSERVER")) return "MSSQL";
        if (dialectUpper.contains("MARIADB")) return "MARIADB";
        return "";
    }

    private String extractVendorFromUrl() {
        if (url == null) return "";

        String urlUpper = url.toUpperCase();
        if (urlUpper.contains(POSTGRES_SQL)) return POSTGRES_SQL;
        if (urlUpper.contains(MYSQL)) return MYSQL;
        if (urlUpper.contains(ORACLE)) return ORACLE;
        return "";
    }

    private String extractVendorFromDriver() {
        if (driver == null) return "";

        String driverUpper = driver.toUpperCase();
        if (driverUpper.contains(POSTGRES_SQL)) return POSTGRES_SQL;
        if (driverUpper.contains(MYSQL)) return MYSQL;
        if (driverUpper.contains(ORACLE)) return ORACLE;
        return "";
    }


    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<String> getIncludes() {
        return includes;
    }

    public void setIncludes(List<String> includes) {
        this.includes = includes;
    }

    public List<String> getExcludes() {
        return excludes;
    }

    public void setExcludes(List<String> excludes) {
        this.excludes = excludes;
    }

    public InputFilterConfig getInputFilter() {
        return inputFilter;
    }

    public void setInputFilter(InputFilterConfig inputFilter) {
        this.inputFilter = inputFilter;
    }

    public NamingStrategyConfig getNamingStrategy() {
        return namingStrategy;
    }

    public void setNamingStrategy(NamingStrategyConfig namingStrategy) {
        this.namingStrategy = namingStrategy;
    }
}
