package com.bsit.codegeneration.model;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

public class CustomGeneratorConstructor extends Constructor {

    public CustomGeneratorConstructor() {
        super(GeneratorConfig.class, new LoaderOptions());
    }

    static class DashToCamelCasePropertyUtils extends PropertyUtils {
        @Override
        public Property getProperty(Class<?> type, String name) {
            String camelCaseName = dashToCamelCase(name);
            return super.getProperty(type, camelCaseName);
        }

        private String dashToCamelCase(String dashed) {
            StringBuilder sb = new StringBuilder();
            boolean upperNext = false;

            for (char c : dashed.toCharArray()) {
                if (c == '-') {
                    upperNext = true;
                } else {
                    sb.append(upperNext ? Character.toUpperCase(c) : c);
                    upperNext = false;
                }
            }
            return sb.toString();
        }
    }
}
