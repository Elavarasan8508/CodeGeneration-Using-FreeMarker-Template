package com.bsit.codegeneration.model;

public class MapperConfig {
    private boolean generate;
    private boolean useRowMapper;
    private boolean useResultsetMapper;
    private String mapperStyle;
    private boolean mapNullsExplicitly;
    private String nullHandlingStrategy;
	public boolean isGenerate() {
		return generate;
	}
	public void setGenerate(boolean generate) {
		this.generate = generate;
	}
	public boolean isUseRowMapper() {
		return useRowMapper;
	}
	public void setUseRowMapper(boolean useRowMapper) {
		this.useRowMapper = useRowMapper;
	}
	public boolean isUseResultsetMapper() {
		return useResultsetMapper;
	}
	public void setUseResultsetMapper(boolean useResultsetMapper) {
		this.useResultsetMapper = useResultsetMapper;
	}
	public String getMapperStyle() {
		return mapperStyle;
	}
	public void setMapperStyle(String mapperStyle) {
		this.mapperStyle = mapperStyle;
	}
	public boolean isMapNullsExplicitly() {
		return mapNullsExplicitly;
	}
	public void setMapNullsExplicitly(boolean mapNullsExplicitly) {
		this.mapNullsExplicitly = mapNullsExplicitly;
	}
	public String getNullHandlingStrategy() {
		return nullHandlingStrategy;
	}
	public void setNullHandlingStrategy(String nullHandlingStrategy) {
		this.nullHandlingStrategy = nullHandlingStrategy;
	}

    
}
