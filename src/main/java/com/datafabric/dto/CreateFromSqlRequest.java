package com.datafabric.dto;

import java.util.List;
import java.util.Map;

public class CreateFromSqlRequest {
  private String sql;
  private List<String> context;
  private String engineName;
  private Map<String, Object> references;

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public List<String> getContext() {
    return context;
  }

  public void setContext(List<String> context) {
    this.context = context;
  }

  public String getEngineName() {
    return engineName;
  }

  public void setEngineName(String engineName) {
    this.engineName = engineName;
  }

  public Map<String, Object> getReferences() {
    return references;
  }

  public void setReferences(Map<String, Object> references) {
    this.references = references;
  }
}
