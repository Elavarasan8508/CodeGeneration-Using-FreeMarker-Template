package com.bsit.codegeneration.model;

public class GeneratorSettings {
	private String name;
    private DatabaseConfig database;
    private TargetConfig target;
    private PojoConfig pojo;
    private DaoConfig dao;
    private MapperConfig mapper;
    private RepositoryConfig repository;
    private PojoConfig pojoConfig;
    private RecordConfig record;
	private AdvancedConfig advanced;
    private SqlTemplatesConfig sqlTemplates;
    private LoggingConfig logging;
    private String activeProfile;
   
    
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public RecordConfig getRecord() {
		return record;
	}
	public void setRecord(RecordConfig record) {
		this.record = record;
	}
	public DatabaseConfig getDatabase() {
		return database;
	}
	public void setDatabase(DatabaseConfig database) {
		this.database = database;
	}
	public TargetConfig getTarget() {
		return target;
	}
	public void setTarget(TargetConfig target) {
		this.target = target;
	}
	public PojoConfig getPojo() {
		return pojo;
	}
	public void setPojo(PojoConfig pojo) {
		this.pojo = pojo;
	}
	public DaoConfig getDao() {
		return dao;
	}
	public void setDao(DaoConfig dao) {
		this.dao = dao;
	}
	public MapperConfig getMapper() {
		return mapper;
	}
	public void setMapper(MapperConfig mapper) {
		this.mapper = mapper;
	}
	public RepositoryConfig getRepository() {
		return repository;
	}
	public void setRepository(RepositoryConfig repository) {
		this.repository = repository;
	}
	public AdvancedConfig getAdvanced() {
		return advanced;
	}
	public void setAdvanced(AdvancedConfig advanced) {
		this.advanced = advanced;
	}
	public SqlTemplatesConfig getSqlTemplates() {
		return sqlTemplates;
	}
	public void setSqlTemplates(SqlTemplatesConfig sqlTemplates) {
		this.sqlTemplates = sqlTemplates;
	}
	public LoggingConfig getLogging() {
		return logging;
	}
	public void setLogging(LoggingConfig logging) {
		this.logging = logging;
	}
	public PojoConfig getDto() {
		return pojo;
	}
	public void setDto(PojoConfig dto) {
		this.pojo = dto;
	}
	public String getActiveProfiles() {
		return activeProfile;
	}
	public void setActiveProfiles(String activeProfiles) {
		this.activeProfile = activeProfiles;
	}


}
