package com.datafabric.dto;

import java.util.List;
import java.util.Map;

public class TransformRequest {
  private String transformType;  // "filter", "sort", "join", "groupBy", "deriveColumn", etc.
  private List<Map<String, Object>> columns;
  private List<Map<String, Object>> values;
  private Map<String, Object> aggregation;
  private String sql;
  
  public TransformRequest() {}
  
  public String getTransformType() {
    return transformType;
  }
  
  public void setTransformType(String transformType) {
    this.transformType = transformType;
  }
  
  public List<Map<String, Object>> getColumns() {
    return columns;
  }
  
  public void setColumns(List<Map<String, Object>> columns) {
    this.columns = columns;
  }
  
  public List<Map<String, Object>> getValues() {
    return values;
  }
  
  public void setValues(List<Map<String, Object>> values) {
    this.values = values;
  }
  
  public Map<String, Object> getAggregation() {
    return aggregation;
  }
  
  public void setAggregation(Map<String, Object> aggregation) {
    this.aggregation = aggregation;
  }
  
  public String getSql() {
    return sql;
  }
  
  public void setSql(String sql) {
    this.sql = sql;
  }
}
