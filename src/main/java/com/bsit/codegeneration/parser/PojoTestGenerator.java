package com.bsit.codegeneration.parser;

import com.bsit.codegeneration.model.*;
import com.bsit.codegeneration.util.Relationship;
import com.bsit.codegeneration.util.StringUtils;
import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ParseProblemException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.expr.*;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.type.ClassOrInterfaceType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Optional;

public class PojoTestGenerator {

    private PojoTestGenerator(){}

    private static final String RELATED_LIST = "relatedList";
    private static final String HASH_CODE = "hashCode";

    public static void generateDtoTest(
            String tableName,
            ResultSet columns,
            DatabaseConfig dbConfig,
            TargetConfig target,
            PojoConfig pojoConfig,
            List<Relationship> relationships,
            List<Relationship> reverseRelationships)
            throws SQLException, IOException {

        NamingStrategyConfig naming = dbConfig.getNamingStrategy();
        String rawClassName = StringUtils.stripPrefix(tableName, naming.getStripPrefixes());
        String dtoClassName = StringUtils.toCamelCase(rawClassName, naming.getUppercaseAcronyms(), true);
        String testClassName = dtoClassName + "Test";

        CompilationUnit cu = new CompilationUnit();
        String packageName = target.getBasePackage() + ".test.pojo";
        cu.setPackageDeclaration(packageName);

        // Add proper imports
        cu.addImport("org.junit.jupiter.api.Test");
        cu.addImport("org.junit.jupiter.api.DisplayName");
        cu.addImport("static org.junit.jupiter.api.Assertions.*");

        // Import from pojo package
        String dtoPackage = target.getBasePackage() + ".pojo." + dtoClassName;
        cu.addImport(dtoPackage);

        ClassOrInterfaceDeclaration testClass = cu.addClass(testClassName).setPublic(true);

        // Attempt to load source analysis (fields, constructors, overrides)
        SourceDtoInfo info = analyzeDtoSource(dtoClassName, target);

        // Fallback: extract from DtoGenerator (keeps backwards compatibility)
        LinkedHashMap<String, String> extracted = PojoGenerator.extractPojoFieldTypes(
                columns, relationships, reverseRelationships, pojoConfig, dbConfig);

        LinkedHashMap<String, String> fieldTypes;
        if (info != null && info.fields != null && !info.fields.isEmpty()) {
            fieldTypes = info.fields;
        } else {
            fieldTypes = extracted;
        }

        if (fieldTypes.isEmpty()) {
            System.out.println("Warning: No fields found for " + tableName + ". Skipping test generation.");
            return;
        }

        // FIXED: Add wrapper class imports
        addWrapperClassImports(cu, fieldTypes);

        // Add test data constants
        addTestDataConstants(testClass, fieldTypes, cu, target);

        // Test default constructor
        generateDefaultConstructorTest(testClass, dtoClassName, fieldTypes);

        // Test getters and setters
        generateGettersSettersTest(testClass, dtoClassName, fieldTypes);

        // Parameterized constructor tests, collecting field-sets
        List<Set<String>> constructorFieldSets = new ArrayList<>();
        if (info != null && !info.constructors.isEmpty()) {
            int ctorIndex = 1;
            for (ConstructorInfo ctor : info.constructors) {
                if (ctor.paramNames.isEmpty()) {
                    continue;
                }
                Set<String> fieldsSet = generateConstructorTestForSourceCtor(
                        testClass, dtoClassName, fieldTypes, ctor, ctorIndex++);
                constructorFieldSets.add(fieldsSet);
            }
        } else {
            generateParameterizedConstructorTestFallback(testClass, dtoClassName, fieldTypes);
        }

        // Test builder if enabled — skip if a constructor already sets all fields
        if (pojoConfig.isAddBuilderPattern()) {
            Set<String> allFields = new HashSet<>(fieldTypes.keySet());
            boolean anyCtorSetsAll = constructorFieldSets.stream()
                    .anyMatch(s -> s.equals(allFields));
            if (!anyCtorSetsAll) {
                generateBuilderTest(testClass, dtoClassName, fieldTypes);
            }
        }

        // Test equals and hashCode only if overridden
        if (info != null && info.hasEquals && info.hasHashCode) {
            generateEqualsHashCodeTest(testClass, dtoClassName, fieldTypes);
        }

        // Test toString only if overridden
        if (info != null && info.hasToString) {
            generateToStringTest(testClass, dtoClassName);
        }

        // Test relational fields if relationships exist
        if (!relationships.isEmpty() || !reverseRelationships.isEmpty()) {
            generateRelationalFieldTests(testClass, dtoClassName,
                    fieldTypes, relationships, reverseRelationships, target, cu, naming);
        }

        // Write to file
        Path outputPath = Paths.get(
                target.getOutputDirectory(),
                packageName.replace(".", "/"),
                testClassName + ".java");
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, cu.toString());
        System.out.println("Generated Test: " + outputPath);
    }

    // FIXED: Add explicit imports for wrapper classes
    private static void addWrapperClassImports(CompilationUnit cu, LinkedHashMap<String, String> fieldTypes) {
        Set<String> typesToImport = new HashSet<>(fieldTypes.values());

        // Add all types from List<> generic types
        for (String type : fieldTypes.values()) {
            if (type.startsWith("List<") && type.endsWith(">")) {
                String innerType = type.substring(type.indexOf('<') + 1, type.lastIndexOf('>')).trim();
                typesToImport.add(innerType);
            }
        }

        // Add imports for common wrapper classes
        for (String type : typesToImport) {
            switch (type) {
                case "Integer":
                    cu.addImport("java.lang.Integer");
                    break;
                case "Float":
                    cu.addImport("java.lang.Float");
                    break;
                case "Double":
                    cu.addImport("java.lang.Double");
                    break;
                case "Long":
                    cu.addImport("java.lang.Long");
                    break;
                case "Boolean":
                    cu.addImport("java.lang.Boolean");
                    break;
                case "Byte":
                    cu.addImport("java.lang.Byte");
                    break;
                case "Short":
                    cu.addImport("java.lang.Short");
                    break;
                case "Character":
                    cu.addImport("java.lang.Character");
                    break;
            }
        }
    }

    // ---------- Source analysis helpers ----------

    private static class ConstructorInfo {
        final List<String> paramNames = new ArrayList<>();
        final List<String> paramTypes = new ArrayList<>();
    }

    private static class SourceDtoInfo {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        List<ConstructorInfo> constructors = new ArrayList<>();
        boolean hasEquals = false;
        boolean hasHashCode = false;
        boolean hasToString = false;
    }

    private static SourceDtoInfo analyzeDtoSource(String dtoClassName, TargetConfig target) {
        List<String> candidateRoots = getCandidateRoots(target);
        String basePkgPath = getBasePackagePath(target);

        for (String root : candidateRoots) {
            if (root == null) continue;
            Path candidate = Paths.get(root, basePkgPath, "pojo", dtoClassName + ".java");
            if (Files.exists(candidate)) {
                return processJavaFile(candidate, dtoClassName);
            }
        }
        return null;
    }

    private static List<String> getCandidateRoots(TargetConfig target) {
        List<String> roots = new ArrayList<>();
        if (target != null && target.getOutputDirectory() != null) {
            roots.add(target.getOutputDirectory());
        }
        roots.add("src/main/java");
        roots.add("src");
        return roots;
    }

    private static String getBasePackagePath(TargetConfig target) {
        if (target != null && target.getBasePackage() != null) {
            return target.getBasePackage().replace('.', '/');
        }
        return "com/bsit/codegeneration";
    }

    private static SourceDtoInfo processJavaFile(Path candidate, String dtoClassName) {
        try {
            CompilationUnit dtoCu = StaticJavaParser.parse(candidate);
            Optional<ClassOrInterfaceDeclaration> clsOpt = dtoCu.getClassByName(dtoClassName);
            if (clsOpt.isPresent()) {
                return extractDtoInfo(clsOpt.get());
            }
        } catch (IOException | ParseProblemException ignored) { }
        return null;
    }

    private static SourceDtoInfo extractDtoInfo(ClassOrInterfaceDeclaration cls) {
        SourceDtoInfo info = new SourceDtoInfo();
        extractFields(cls, info);
        extractConstructors(cls, info);
        extractMethods(cls, info);
        return info;
    }

    private static void extractFields(ClassOrInterfaceDeclaration cls, SourceDtoInfo info) {
        for (BodyDeclaration<?> member : cls.getMembers()) {
            if (!(member instanceof FieldDeclaration)) continue;
            FieldDeclaration fd = (FieldDeclaration) member;
            for (VariableDeclarator var : fd.getVariables()) {
                info.fields.put(var.getNameAsString(), var.getType().toString());
            }
        }
    }

    private static void extractConstructors(ClassOrInterfaceDeclaration cls, SourceDtoInfo info) {
        for (ConstructorDeclaration cd : cls.getConstructors()) {
            ConstructorInfo ci = new ConstructorInfo();
            cd.getParameters().forEach(p -> {
                ci.paramNames.add(p.getNameAsString());
                ci.paramTypes.add(p.getType().toString());
            });
            info.constructors.add(ci);
        }
    }

    private static void extractMethods(ClassOrInterfaceDeclaration cls, SourceDtoInfo info) {
        for (MethodDeclaration md : cls.getMethods()) {
            String methodName = md.getNameAsString();
            if ("equals".equals(methodName) && md.getParameters().size() == 1) {
                info.hasEquals = true;
            } else if (HASH_CODE.equals(methodName) && md.getParameters().isEmpty()) {
                info.hasHashCode = true;
            } else if ("toString".equals(methodName) && md.getParameters().isEmpty()) {
                info.hasToString = true;
            }
        }
    }


    // ---------- Test generation helpers ----------

    private static void addTestDataConstants(
            ClassOrInterfaceDeclaration testClass,
            LinkedHashMap<String, String> fieldTypes,
            CompilationUnit cu,
            TargetConfig target) {
        int counter = 1;
        for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
            String field = entry.getKey();
            String type = entry.getValue();
            String constantName = field.toLowerCase();
            String testValue = getRealisticTestValue(type, field, counter++, cu, target);
            FieldDeclaration fd = testClass.addField(
                    type, constantName,
                    Modifier.Keyword.PRIVATE,
                    Modifier.Keyword.STATIC,
                    Modifier.Keyword.FINAL);
            fd.getVariable(0).setInitializer(testValue);
        }
    }

    private static void generateDefaultConstructorTest(
            ClassOrInterfaceDeclaration testClass,
            String dtoClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testDefaultConstructor", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should create object with default constructor and null/default values")));

        BlockStmt body = new BlockStmt();
        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, dtoClassName),
                        "obj",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), new NodeList<>())))));
        body.addStatement(new ExpressionStmt(new MethodCallExpr(null, "assertNotNull",
                new NodeList<>(new NameExpr("obj")))));

        for (String field : fieldTypes.keySet()) {
            String getter = "get" + StringUtils.capitalize(field);
            body.addStatement(new ExpressionStmt(new MethodCallExpr(null, "assertNull",
                    new NodeList<>(new MethodCallExpr(new NameExpr("obj"), getter)))));
        }
        m.setBody(body);
    }

    private static void generateGettersSettersTest(
            ClassOrInterfaceDeclaration testClass,
            String dtoClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testGettersAndSetters", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should get and set all properties correctly")));

        BlockStmt body = new BlockStmt();
        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, dtoClassName),
                        "obj",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), new NodeList<>())))));

        if (fieldTypes != null && body != null) {
            for (String field : fieldTypes.keySet()) {
                if (field == null) continue;
                String setter = "set" + StringUtils.capitalize(field);
                String getter = "get" + StringUtils.capitalize(field);
                String constant = field.toLowerCase();

                body.addStatement(new ExpressionStmt(new MethodCallExpr(
                        new NameExpr("obj"), setter, new NodeList<>(new NameExpr(constant)))));

                body.addStatement(new ExpressionStmt(new MethodCallExpr(
                        null, "assertEquals",
                        new NodeList<>(new NameExpr(constant), new MethodCallExpr(new NameExpr("obj"), getter)))));
            }
        }

        m.setBody(body);
    }

    private static Set<String> generateConstructorTestForSourceCtor(
            ClassOrInterfaceDeclaration testClass,
            String dtoClassName,
            LinkedHashMap<String, String> fieldTypes,
            ConstructorInfo ctor,
            int ctorIndex) {

        String testName = "testParameterizedConstructor" + ctorIndex;
        MethodDeclaration m = testClass.addMethod(testName, Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should create object using constructor #" + ctorIndex)));

        BlockStmt body = new BlockStmt();
        NodeList<Expression> args = new NodeList<>();
        Set<String> fieldsSet = new HashSet<>();

        for (int i = 0; i < ctor.paramNames.size(); i++) {
            String pName = ctor.paramNames.get(i);
            String pType = ctor.paramTypes.get(i);
            String matched = null;

            if (fieldTypes.containsKey(pName)) {
                matched = pName;
            } else {
                for (Map.Entry<String, String> fe : fieldTypes.entrySet()) {
                    if (typesMatch(pType, fe.getValue())) {
                        matched = fe.getKey();
                        break;
                    }
                }
            }

            if (matched != null) {
                fieldsSet.add(matched);
                args.add(new NameExpr(matched.toLowerCase()));
            } else {
                args.add(new NameExpr("null"));
            }
        }

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, dtoClassName),
                        "obj",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), args)))));
        body.addStatement(new ExpressionStmt(new MethodCallExpr(null, "assertNotNull",
                new NodeList<>(new NameExpr("obj")))));

        if (fieldTypes != null && fieldsSet != null && body != null) {
            for (String field : fieldTypes.keySet()) {
                if (field == null) continue;
                String getter = "get" + StringUtils.capitalize(field);
                String constant = field.toLowerCase();

                if (fieldsSet.contains(field)) {
                    body.addStatement(new ExpressionStmt(new MethodCallExpr(
                            null, "assertEquals",
                            new NodeList<>(new NameExpr(constant),
                                    new MethodCallExpr(new NameExpr("obj"), getter)))));
                } else {
                    body.addStatement(new ExpressionStmt(new MethodCallExpr(
                            null, "assertNull",
                            new NodeList<>(new MethodCallExpr(new NameExpr("obj"), getter)))));
                }
            }
        }

        m.setBody(body);
        return fieldsSet;
    }

    private static void generateParameterizedConstructorTestFallback(
            ClassOrInterfaceDeclaration testClass,
            String dtoClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testParameterizedConstructorFull", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should create object with full parameterized constructor")));

        BlockStmt body = new BlockStmt();
        NodeList<Expression> args = new NodeList<>();
        for (String field : fieldTypes.keySet()) {
            args.add(new NameExpr(field.toLowerCase()));
        }

        if (!args.isEmpty()) {
            body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                    new VariableDeclarator(
                            new ClassOrInterfaceType(null, dtoClassName),
                            "obj",
                            new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), args)))));
            body.addStatement(new ExpressionStmt(new MethodCallExpr(null, "assertNotNull",
                    new NodeList<>(new NameExpr("obj")))));
            if(fieldTypes != null && body != null) {
                for (String field : fieldTypes.keySet()) {
                    if(field == null) continue;
                    String getter = "get" + StringUtils.capitalize(field);
                    String constant = field.toLowerCase();
                    body.addStatement(new ExpressionStmt(new MethodCallExpr(
                            null, "assertEquals",
                            new NodeList<>(new NameExpr(constant),
                                    new MethodCallExpr(new NameExpr("obj"), getter)))));
                }
            }
        }

        m.setBody(body);
    }

    private static void generateBuilderTest(
            ClassOrInterfaceDeclaration testClass,
            String dtoClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testBuilder", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should build object using builder pattern")));

        BlockStmt body = new BlockStmt();
        Expression builderExpr = new ObjectCreationExpr(
                null,
                new ClassOrInterfaceType(null, dtoClassName + ".Builder"),
                new NodeList<>());

        for (String field : fieldTypes.keySet()) {
            builderExpr = new MethodCallExpr(
                    builderExpr,
                    field,
                    new NodeList<>(new NameExpr(field.toLowerCase())));
        }
        builderExpr = new MethodCallExpr(builderExpr, "build");

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, dtoClassName),
                        "obj",
                        builderExpr))));
        body.addStatement(new ExpressionStmt(new MethodCallExpr(null, "assertNotNull",
                new NodeList<>(new NameExpr("obj")))));
        if (fieldTypes != null && body != null) {
            for (String field : fieldTypes.keySet()) {
                if(field == null) continue;
                String getter = "get" + StringUtils.capitalize(field);
                String constant = field.toLowerCase();
                body.addStatement(new ExpressionStmt(new MethodCallExpr(
                        null, "assertEquals",
                        new NodeList<>(new NameExpr(constant),
                                new MethodCallExpr(new NameExpr("obj"), getter)))));
            }
        }
        m.setBody(body);
    }

    private static void generateEqualsHashCodeTest(
            ClassOrInterfaceDeclaration testClass,
            String dtoClassName,
            LinkedHashMap<String, String> fieldTypes) {

        MethodDeclaration m = testClass.addMethod("testEqualsAndHashCode", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should implement equals and hashCode correctly")));

        BlockStmt body = new BlockStmt();

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, dtoClassName),
                        "obj1",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), new NodeList<>())))));
        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, dtoClassName),
                        "obj2",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), new NodeList<>())))));
        if(fieldTypes != null && body != null) {
            for (String field : fieldTypes.keySet()) {
                if(field == null) continue;
                String setter = "set" + StringUtils.capitalize(field);
                String constant = field.toLowerCase();
                body.addStatement(new ExpressionStmt(new MethodCallExpr(
                        new NameExpr("obj1"),
                        setter,
                        new NodeList<>(new NameExpr(constant)))));
                body.addStatement(new ExpressionStmt(new MethodCallExpr(
                        new NameExpr("obj2"),
                        setter,
                        new NodeList<>(new NameExpr(constant)))));
            }
        }

        body.addStatement(new ExpressionStmt(new MethodCallExpr(
                null, "assertEquals",
                new NodeList<>(new NameExpr("obj1"), new NameExpr("obj2")))));

        body.addStatement(new ExpressionStmt(new MethodCallExpr(
                null, "assertEquals",
                new NodeList<>(
                        new MethodCallExpr(new NameExpr("obj1"), HASH_CODE),
                        new MethodCallExpr(new NameExpr("obj2"), HASH_CODE)))));

        m.setBody(body);
    }

    private static void generateToStringTest(
            ClassOrInterfaceDeclaration testClass,
            String dtoClassName) {

        MethodDeclaration m = testClass.addMethod("testToString", Modifier.Keyword.PUBLIC);
        m.addAnnotation("Test");
        m.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("DisplayName"),
                new StringLiteralExpr("Should have meaningful toString implementation")));

        BlockStmt body = new BlockStmt();
        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, dtoClassName),
                        "obj",
                        new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), new NodeList<>())))));

        body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                new VariableDeclarator(
                        new ClassOrInterfaceType(null, "String"),
                        "result",
                        new MethodCallExpr(new NameExpr("obj"), "toString")))));
        body.addStatement(new ExpressionStmt(new MethodCallExpr(
                null, "assertNotNull",
                new NodeList<>(new NameExpr("result")))));

        m.setBody(body);
    }

    private static void generateRelationalFieldTests(
            ClassOrInterfaceDeclaration testClass,
            String dtoClassName,
            LinkedHashMap<String, String> fieldTypes,
            List<Relationship> relationships,
            List<Relationship> reverseRelationships,
            TargetConfig target,
            CompilationUnit cu,
            NamingStrategyConfig naming) {

        // Forward relationships
        for (Relationship rel : relationships) {
            String rawName = StringUtils.stripPrefix(rel.getRelatedTable(), naming.getStripPrefixes());
            String relatedDto = StringUtils.toCamelCase(rawName, naming.getUppercaseAcronyms(), true);
            String fk = StringUtils.toCamelCase(rel.getFkColumn(), naming.getUppercaseAcronyms(), false);
            String fieldName = fk.toLowerCase().endsWith("id")
                    ? fk.substring(0, fk.length() - 2)
                    : fk;
            if (fieldName.isEmpty()) {
                fieldName = StringUtils.toCamelCase(rel.getRelatedTable(), naming.getUppercaseAcronyms(), false);
            }
            String suffix = StringUtils.capitalize(fieldName);
            String testName = "test" + suffix + "Relationship";

            if (!testClass.getMethodsByName(testName).isEmpty()) {
                continue;
            }

            cu.addImport(target.getBasePackage() + ".pojo." + relatedDto);

            MethodDeclaration m = testClass.addMethod(testName, Modifier.Keyword.PUBLIC);
            m.addAnnotation("Test");
            m.addAnnotation(new SingleMemberAnnotationExpr(
                    new Name("DisplayName"),
                    new StringLiteralExpr("Should set and get " + fieldName + " relationship correctly")));

            BlockStmt body = new BlockStmt();
            body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                    new VariableDeclarator(
                            new ClassOrInterfaceType(null, dtoClassName),
                            "obj",
                            new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), new NodeList<>())))));

            String constant = fieldName.toLowerCase();
            Expression init = fieldTypes.containsKey(fieldName)
                    ? new NameExpr(constant)
                    : new ObjectCreationExpr(null, new ClassOrInterfaceType(null, relatedDto), new NodeList<>());
            body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                    new VariableDeclarator(
                            new ClassOrInterfaceType(null, relatedDto),
                            "relatedObj",
                            init))));

            body.addStatement(new ExpressionStmt(new MethodCallExpr(
                    new NameExpr("obj"),
                    "set" + suffix,
                    new NodeList<>(new NameExpr("relatedObj")))));

            body.addStatement(new ExpressionStmt(new MethodCallExpr(
                    null, "assertSame",
                    new NodeList<>(new NameExpr("relatedObj"),
                            new MethodCallExpr(new NameExpr("obj"), "get" + suffix)))));

            m.setBody(body);
        }

        // Reverse relationships
        for (Relationship rel : reverseRelationships) {
            String rawName = StringUtils.stripPrefix(rel.getRelatedTable(), naming.getStripPrefixes());
            String relatedDto = StringUtils.toCamelCase(rawName, naming.getUppercaseAcronyms(), true);
            String listField = StringUtils.toCamelCase(rel.getRelatedTable(), naming.getUppercaseAcronyms(), false) + "List";
            String testName = "test" + StringUtils.capitalize(listField) + "Relationship";

            if (!testClass.getMethodsByName(testName).isEmpty()) {
                continue;
            }

            cu.addImport("java.util.List");
            cu.addImport("java.util.ArrayList");
            cu.addImport(target.getBasePackage() + ".pojo." + relatedDto);

            MethodDeclaration m = testClass.addMethod(testName, Modifier.Keyword.PUBLIC);
            m.addAnnotation("Test");
            m.addAnnotation(new SingleMemberAnnotationExpr(
                    new Name("DisplayName"),
                    new StringLiteralExpr("Should manage " + listField + " relationship correctly")));

            BlockStmt body = new BlockStmt();
            body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                    new VariableDeclarator(
                            new ClassOrInterfaceType(null, dtoClassName),
                            "obj",
                            new ObjectCreationExpr(null, new ClassOrInterfaceType(null, dtoClassName), new NodeList<>())))));

            String constant = listField.toLowerCase();
            Expression init = fieldTypes.containsKey(listField)
                    ? new NameExpr(constant)
                    : new ObjectCreationExpr(null,
                    new ClassOrInterfaceType(null, "ArrayList<>"),
                    new NodeList<>());
            body.addStatement(new ExpressionStmt(new VariableDeclarationExpr(
                    new VariableDeclarator(
                            new ClassOrInterfaceType(null, "List<" + relatedDto + ">"),
                            RELATED_LIST,
                            init))));

            if (!fieldTypes.containsKey(listField)) {
                body.addStatement(new ExpressionStmt(new MethodCallExpr(
                        new NameExpr(RELATED_LIST),
                        "add",
                        new NodeList<>(new ObjectCreationExpr(null, new ClassOrInterfaceType(null, relatedDto), new NodeList<>())))));
            }

            body.addStatement(new ExpressionStmt(new MethodCallExpr(
                    new NameExpr("obj"),
                    "set" + StringUtils.capitalize(listField),
                    new NodeList<>(new NameExpr(RELATED_LIST)))));

            body.addStatement(new ExpressionStmt(new MethodCallExpr(
                    null, "assertNotNull",
                    new NodeList<>(new MethodCallExpr(new NameExpr("obj"), "get" + StringUtils.capitalize(listField))))));
            body.addStatement(new ExpressionStmt(new MethodCallExpr(
                    null, "assertEquals",
                    new NodeList<>(new NameExpr(RELATED_LIST),
                            new MethodCallExpr(new NameExpr("obj"), "get" + StringUtils.capitalize(listField))))));

            m.setBody(body);
        }
    }

    // ---------- Utilities ----------

    private static boolean typesMatch(String pType, String fType) {
        if (pType == null || fType == null) return false;
        String pSimple = pType.contains(".")
                ? pType.substring(pType.lastIndexOf('.') + 1)
                : pType;
        String fSimple = fType.contains(".")
                ? fType.substring(fType.lastIndexOf('.') + 1)
                : fType;
        return pType.equals(fType)
                || pSimple.equalsIgnoreCase(fSimple)
                || pType.replaceAll("\\s+", "").equalsIgnoreCase(fType.replaceAll("\\s+", ""));
    }

    private static String getRealisticTestValue(
            String type,
            String fieldName,
            int counter,
            CompilationUnit cu,
            TargetConfig target) {

        if (type == null) return "null";
        String lower = type.toLowerCase();

        if (type.startsWith("List<") && type.endsWith(">")) {
            String inner = type.substring(type.indexOf('<') + 1, type.lastIndexOf('>')).trim();
            if (Character.isUpperCase(inner.charAt(0))) {
                if (target != null) cu.addImport(target.getBasePackage() + ".pojo." + inner);
                cu.addImport("java.util.ArrayList");
                cu.addImport("java.util.Arrays");
                cu.addImport("java.util.List");
                return "new ArrayList<>(Arrays.asList(new " + inner + "()))";
            } else {
                cu.addImport("java.util.ArrayList");
                cu.addImport("java.util.List");
                return "new ArrayList<>()";
            }
        }

        if (Character.isUpperCase(type.charAt(0)) && !type.contains(".") && !type.equals("String")
                && !Arrays.asList("Integer", "Float", "Double", "Long", "Boolean", "Byte", "Short", "Character").contains(type)) {
            if (target != null) cu.addImport(target.getBasePackage() + ".pojo." + type);
            return "new " + type + "()";
        }

        switch (lower) {
            case "string":
                if (fieldName.toLowerCase().contains("name")) {
                    return "\"Test Name " + counter + "\"";
                }
                if (fieldName.toLowerCase().contains("email")) {
                    return "\"test" + counter + "@example.com\"";
                }
                if (fieldName.toLowerCase().contains("description")) {
                    return "\"Test description for " + fieldName + "\"";
                }
                return "\"TestValue" + counter + "\"";

            case "integer", "int":
                return String.valueOf(counter * 10);

            case "long":
                return (counter * 100L) + "L";

            case "float":
                return (counter * 1.5f) + "f";

            case "double":
                return (counter * 2.5) + "d";

            case "boolean":
                return counter % 2 == 0 ? "true" : "false";

            case "java.time.localdatetime":
                cu.addImport("java.time.LocalDateTime");
                return "LocalDateTime.of(2023, "
                        + Math.max(1, counter % 12) + ", "
                        + Math.max(1, counter % 28)
                        + ", 10, 30, 0)";

            case "java.time.localdate":
                cu.addImport("java.time.LocalDate");
                return "LocalDate.of(2023, "
                        + Math.max(1, counter % 12) + ", "
                        + Math.max(1, counter % 28) + ")";

            case "java.util.map<string, object>":
                cu.addImport("java.util.HashMap");
                cu.addImport("java.util.Map");
                return "new HashMap<>()";

            case "java.math.bigdecimal":
                cu.addImport("java.math.BigDecimal");
                return "new BigDecimal(\"" + (counter * 10.99) + "\")";

            default:
                if (lower.startsWith("list<")) {
                    cu.addImport("java.util.ArrayList");
                    return "new ArrayList<>()";
                }
                return "null";
        }
    }
}
