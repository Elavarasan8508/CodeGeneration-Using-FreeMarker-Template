package com.bsit.codegeneration.model;

public class RepositoryConfig {
    private boolean generate;
    private String annotation;
    private String transactionManager;
	public boolean isGenerate() {
		return generate;
	}
	public void setGenerate(boolean generate) {
		this.generate = generate;
	}
	public String getAnnotation() {
		return annotation;
	}
	public void setAnnotation(String annotation) {
		this.annotation = annotation;
	}
	public String getTransactionManager() {
		return transactionManager;
	}
	public void setTransactionManager(String transactionManager) {
		this.transactionManager = transactionManager;
	}

    
}
