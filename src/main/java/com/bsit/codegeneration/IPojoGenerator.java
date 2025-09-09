package com.bsit.codegeneration;

import java.sql.Connection;

public interface IPojoGenerator {
    void generate(Connection conn, String schema, String packageName, String outputDir) throws Exception;
}
