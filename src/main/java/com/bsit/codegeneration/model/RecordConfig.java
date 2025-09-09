package com.bsit.codegeneration.model;

import java.util.List;

public class RecordConfig {
    private boolean generate;
    private boolean builderPattern; 
    private String packageName;      
    private List<String> stripPrefixes; 
    private boolean useJavaTime;     
    private List<String> annotations;
    private boolean includeRelationships;
    private boolean includeReverseRelationships;
    private boolean removeForeignKeyIdFields;

    public boolean isRemoveForeignKeyIdFields() {
		return removeForeignKeyIdFields;
	}

	public void setRemoveForeignKeyIdFields(boolean removeForeignKeyIdFields) {
		this.removeForeignKeyIdFields = removeForeignKeyIdFields;
	}

	public boolean isIncludeRelationships() {
		return includeRelationships;
	}

	public void setIncludeRelationships(boolean includeRelationships) {
		this.includeRelationships = includeRelationships;
	}

	public boolean isIncludeReverseRelationships() {
		return includeReverseRelationships;
	}

	public void setIncludeReverseRelationships(boolean includeReverseRelationships) {
		this.includeReverseRelationships = includeReverseRelationships;
	}

	public boolean isGenerate() {
        return generate;
    }

    public void setGenerate(boolean generate) {
        this.generate = generate;
    }

    public boolean isBuilderPattern() {
        return builderPattern;
    }

    public void setBuilderPattern(boolean builderPattern) {
        this.builderPattern = builderPattern;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public List<String> getStripPrefixes() {
        return stripPrefixes;
    }

    public void setStripPrefixes(List<String> stripPrefixes) {
        this.stripPrefixes = stripPrefixes;
    }

    public boolean isUseJavaTime() {
        return useJavaTime;
    }

    public void setUseJavaTime(boolean useJavaTime) {
        this.useJavaTime = useJavaTime;
    }

    public List<String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(List<String> annotations) {
        this.annotations = annotations;
    }
}
