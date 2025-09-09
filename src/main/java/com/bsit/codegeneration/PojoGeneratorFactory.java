package com.bsit.codegeneration;

import com.bsit.codegeneration.parser.PojoGenerator;
import com.bsit.codegeneration.freemarker.FreeMarkerPojoGenerator;

public class PojoGeneratorFactory {
    public static IPojoGenerator create(String type) {
        return switch (type.toLowerCase()) {
            case "javaparser" -> new PojoGenerator();
            case "freemarker" -> new FreeMarkerPojoGenerator();
            default -> throw new IllegalArgumentException("Unknown generator: " + type);
        };
    }
}
