package com.bsit.codegeneration.model;

import java.util.List;

public class NamingStrategyConfig {
    private static List<String> stripPrefixes;
    private boolean convertToCamelCase;
    private static List<String> uppercaseAcronyms;

    public static List<String> getStripPrefixes() {
        return stripPrefixes;
    }

    public void setStripPrefixes(List<String> stripPrefixes) {
        this.stripPrefixes = stripPrefixes;
    }

    public boolean isConvertToCamelCase() {
        return convertToCamelCase;
    }

    public void setConvertToCamelCase(boolean convertToCamelCase) {
        this.convertToCamelCase = convertToCamelCase;
    }

    public static List<String> getUppercaseAcronyms() {
        return uppercaseAcronyms;
    }

    public void setUppercaseAcronyms(List<String> uppercaseAcronyms) {
        this.uppercaseAcronyms = uppercaseAcronyms;
    }
}
