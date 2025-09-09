package com.bsit.codegeneration;

import com.bsit.codegeneration.metadata.YamlParser;

public class App {
    public static void main(String[] args) {
        YamlParser yamlParser = new YamlParser();
        try {
            yamlParser.generate();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse the yaml" + e);
        }
    }
}
