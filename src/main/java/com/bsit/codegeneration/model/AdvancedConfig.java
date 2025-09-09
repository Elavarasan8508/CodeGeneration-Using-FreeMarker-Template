package com.bsit.codegeneration.model;

import java.util.List;

public class AdvancedConfig {
    private String enumStrategy;
    private boolean generateSchemaAsJavaPackage;
    private boolean generateStaticQueries;
    private boolean generateMetadata;
    private List<CustomTypeMapping> customTypeMappings;
	public String getEnumStrategy() {
		return enumStrategy;
	}
	public void setEnumStrategy(String enumStrategy) {
		this.enumStrategy = enumStrategy;
	}
	public boolean isGenerateSchemaAsJavaPackage() {
		return generateSchemaAsJavaPackage;
	}
	public void setGenerateSchemaAsJavaPackage(boolean generateSchemaAsJavaPackage) {
		this.generateSchemaAsJavaPackage = generateSchemaAsJavaPackage;
	}
	public boolean isGenerateStaticQueries() {
		return generateStaticQueries;
	}
	public void setGenerateStaticQueries(boolean generateStaticQueries) {
		this.generateStaticQueries = generateStaticQueries;
	}
	public boolean isGenerateMetadata() {
		return generateMetadata;
	}
	public void setGenerateMetadata(boolean generateMetadata) {
		this.generateMetadata = generateMetadata;
	}
	public List<CustomTypeMapping> getCustomTypeMappings() {
		return customTypeMappings;
	}
	public void setCustomTypeMappings(List<CustomTypeMapping> customTypeMappings) {
		this.customTypeMappings = customTypeMappings;
	}

   
}
