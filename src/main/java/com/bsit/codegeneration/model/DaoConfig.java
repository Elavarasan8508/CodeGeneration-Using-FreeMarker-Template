package com.bsit.codegeneration.model;

public class DaoConfig {
  private boolean generate;
  private String style;
  private boolean useNamedParameters;
  private boolean generateCrud;
  private boolean generateBatchOps;
  private boolean generatePaginationSupport;
  private boolean generateSoftDeleteSupport;
  private String baseClass;

  public boolean isGenerate() {
    return generate;
  }

  public void setGenerate(boolean generate) {
    this.generate = generate;
  }

  public String getStyle() {
    return style;
  }

  public void setStyle(String style) {
    this.style = style;
  }

  public boolean isUseNamedParameters() {
    return useNamedParameters;
  }

  public void setUseNamedParameters(boolean useNamedParameters) {
    this.useNamedParameters = useNamedParameters;
  }

  public boolean isGenerateCrud() {
    return generateCrud;
  }

  public void setGenerateCrud(boolean generateCrud) {
    this.generateCrud = generateCrud;
  }

  public boolean isGenerateBatchOps() {
    return generateBatchOps;
  }

  public void setGenerateBatchOps(boolean generateBatchOps) {
    this.generateBatchOps = generateBatchOps;
  }

  public boolean isGeneratePaginationSupport() {
    return generatePaginationSupport;
  }

  public void setGeneratePaginationSupport(boolean generatePaginationSupport) {
    this.generatePaginationSupport = generatePaginationSupport;
  }

  public boolean isGenerateSoftDeleteSupport() {
    return generateSoftDeleteSupport;
  }

  public void setGenerateSoftDeleteSupport(boolean generateSoftDeleteSupport) {
    this.generateSoftDeleteSupport = generateSoftDeleteSupport;
  }

  public String getBaseClass() {
    return baseClass;
  }

  public void setBaseClass(String baseClass) {
    this.baseClass = baseClass;
  }
}
