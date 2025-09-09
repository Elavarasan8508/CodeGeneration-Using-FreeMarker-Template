package com.bsit.codegeneration.model;

public class SqlTemplatesConfig {
    private boolean enabled;
    private String outputDirectory;
    private boolean generatePerMethod;
    private String templateNaming;
    private boolean dialectSpecificVariants;
	public boolean isEnabled() {
		return enabled;
	}
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	public String getOutputDirectory() {
		return outputDirectory;
	}
	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}
	public boolean isGeneratePerMethod() {
		return generatePerMethod;
	}
	public void setGeneratePerMethod(boolean generatePerMethod) {
		this.generatePerMethod = generatePerMethod;
	}
	public String getTemplateNaming() {
		return templateNaming;
	}
	public void setTemplateNaming(String templateNaming) {
		this.templateNaming = templateNaming;
	}
	public boolean isDialectSpecificVariants() {
		return dialectSpecificVariants;
	}
	public void setDialectSpecificVariants(boolean dialectSpecificVariants) {
		this.dialectSpecificVariants = dialectSpecificVariants;
	}

    
}
