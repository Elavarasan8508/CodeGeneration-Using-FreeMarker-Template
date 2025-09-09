package ${packageName};

<#-- Determine required imports -->
<#assign needBigDecimal = false>
<#assign needLocalDate = false>
<#assign needLocalDateTime = false>
<#assign needLocalTime = false>
<#assign needSqlDate = false>
<#assign needSqlTime = false>
<#assign needSqlTimestamp = false>
<#assign needList = false>
<#assign needMap = false>
<#assign needUUID = false>

<#list fields as field>
<#if field.type == "java.math.BigDecimal">
<#assign needBigDecimal = true>
</#if>
<#if field.type == "java.time.LocalDate">
<#assign needLocalDate = true>
</#if>
<#if field.type == "java.time.LocalDateTime">
<#assign needLocalDateTime = true>
</#if>
<#if field.type == "java.time.LocalTime">
<#assign needLocalTime = true>
</#if>
<#if field.type == "java.sql.Date">
<#assign needSqlDate = true>
</#if>
<#if field.type == "java.sql.Time">
<#assign needSqlTime = true>
</#if>
<#if field.type == "java.sql.Timestamp">
<#assign needSqlTimestamp = true>
</#if>
<#if field.type == "java.util.UUID">
<#assign needUUID = true>
</#if>
<#if field.type?starts_with("java.util.Map")>
<#assign needMap = true>
</#if>
<#if field.isRelation?? && field.isRelation && (field.relationType == "OneToMany" || field.relationType == "ManyToMany")>
<#assign needList = true>
</#if>
<#if field.type?starts_with("List<")>
<#assign needList = true>
</#if>
</#list>

<#-- Generate imports -->
<#if needBigDecimal>
import java.math.BigDecimal;
</#if>
<#if needLocalDate>
import java.time.LocalDate;
</#if>
<#if needLocalDateTime>
import java.time.LocalDateTime;
</#if>
<#if needLocalTime>
import java.time.LocalTime;
</#if>
<#if needSqlDate>
import java.sql.Date;
</#if>
<#if needSqlTime>
import java.sql.Time;
</#if>
<#if needSqlTimestamp>
import java.sql.Timestamp;
</#if>
<#if needUUID>
import java.util.UUID;
</#if>
<#if needList>
import java.util.List;
import java.util.ArrayList;
</#if>
<#if needMap>
import java.util.Map;
</#if>

public class ${className} {

<#-- Generate fields -->
<#list fields as field>
<#if field.isRelation?? && field.isRelation>
<#if field.relationType == "OneToMany" || field.relationType == "ManyToMany">
private List<${field.referenceClass}> ${field.name} = new ArrayList<>();
        <#else>
    private ${field.referenceClass} ${field.name};
        </#if>
    <#else>
    private ${field.type} ${field.name};
    </#if>
    </#list>

    <#-- Default constructor -->
    public ${className}() {
}

    <#-- Parameterized constructor (excluding List fields for relations) -->
    <#assign constructorFields = []>
    <#list fields as field>
        <#if field.isRelation?? && field.isRelation>
            <#if field.relationType != "OneToMany" && field.relationType != "ManyToMany">
                <#assign constructorFields = constructorFields + [field]>
            </#if>
        <#else>
            <#assign constructorFields = constructorFields + [field]>
        </#if>
    </#list>

    <#if constructorFields?size &gt; 0>
    public ${className}(<#list constructorFields as field><#if field.isRelation?? && field.isRelation>${field.referenceClass}<#else>${field.type}</#if> ${field.name}<#if field_has_next>, </#if></#list>) {
<#list constructorFields as field>
this.${field.name} = ${field.name};
        </#list>
    }
    </#if>

    <#-- Generate getters and setters -->
    <#list fields as field>
    <#if field.isRelation?? && field.isRelation>
        <#if field.relationType == "OneToMany" || field.relationType == "ManyToMany">
    public List<${field.referenceClass}> get${field.name?cap_first}() {
return ${field.name};
    }

    public void set${field.name?cap_first}(List<${field.referenceClass}> ${field.name}) {
this.${field.name} = ${field.name};
    }

    public void add${field.referenceClass}(${field.referenceClass} ${field.referenceClass?lower_case}) {
if (this.${field.name} == null) {
this.${field.name} = new ArrayList<>();
        }
        this.${field.name}.add(${field.referenceClass?lower_case});
    }
        <#else>
    public ${field.referenceClass} get${field.name?cap_first}() {
return ${field.name};
    }

    public void set${field.name?cap_first}(${field.referenceClass} ${field.name}) {
this.${field.name} = ${field.name};
    }
        </#if>
    <#else>
    public ${field.type} get${field.name?cap_first}() {
return ${field.name};
    }

    public void set${field.name?cap_first}(${field.type} ${field.name}) {
this.${field.name} = ${field.name};
    }
    </#if>

    </#list>
}
