package com.bsit.codegeneration.model;

public class TargetConfig {
  private String basePackage;
  private String outputDirectory;
  private String recordPackage;
  private String daoPackage;
  private String mapperPackage;
  private String repositoryPackage;
  private boolean overwrite;
  private boolean formatSource;

  public String getBasePackage() {
    return basePackage;
  }

    public TargetConfig() {
    }

    public TargetConfig(String outputDirectory, String basePackage) {
        this.outputDirectory = outputDirectory;
        this.basePackage = basePackage;
    }


    public void setBasePackage(String basePackage) {
    this.basePackage = basePackage;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setOutputDirectory(String outputDirectory) {
    this.outputDirectory = outputDirectory;
  }

  public String getRecordPackage() {
    return recordPackage;
  }

  public void setRecordPackage(String recordPackage) {
    this.recordPackage = recordPackage;
  }

  public String getDaoPackage() {
    return daoPackage;
  }

  public void setDaoPackage(String daoPackage) {
    this.daoPackage = daoPackage;
  }

  public String getMapperPackage() {
    return mapperPackage;
  }

  public void setMapperPackage(String mapperPackage) {
    this.mapperPackage = mapperPackage;
  }

  public String getRepositoryPackage() {
    return repositoryPackage;
  }

  public void setRepositoryPackage(String repositoryPackage) {
    this.repositoryPackage = repositoryPackage;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public boolean isFormatSource() {
    return formatSource;
  }

  public void setFormatSource(boolean formatSource) {
    this.formatSource = formatSource;
  }
}
