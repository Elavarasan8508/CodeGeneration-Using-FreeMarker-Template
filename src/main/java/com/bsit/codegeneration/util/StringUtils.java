package com.bsit.codegeneration.util;

import java.util.List;

public final class StringUtils {

    private StringUtils() {}

    public static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    public static String stripPrefix(String name, List<String> prefixes) {
        if (name == null || prefixes == null) return name;
        for (String prefix : prefixes) {
            if (name.startsWith(prefix)) {
                return name.substring(prefix.length());
            }
        }
        return name;
    }

    public static String toCamelCase(String name, List<String> acronyms, boolean capitalizeFirst) {
        if (name == null || name.isEmpty()) return name;
        boolean leading = name.startsWith("_");
        boolean trailing = name.endsWith("_");
        String clean = name;
        if (leading) clean = clean.substring(1);
        if (trailing) clean = clean.substring(0, clean.length() - 1);

        String[] parts = clean.toLowerCase().split("_");
        StringBuilder sb = new StringBuilder();
        if (leading) sb.append("_");

        for (int i = 0; i < parts.length; i++) {
            String p = parts[i];
            if (p.isEmpty()) continue;
            boolean isAcronym = acronyms != null && acronyms.contains(p.toUpperCase());
            if (isAcronym) {
                sb.append(p.toUpperCase());
            } else if (i == 0 && !capitalizeFirst) {
                sb.append(p);
            } else {
                sb.append(Character.toUpperCase(p.charAt(0))).append(p.substring(1));
            }
        }
        if (trailing) sb.append("_");
        return sb.toString();
    }
}
