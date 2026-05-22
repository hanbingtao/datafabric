package com.datafabric.dto;

import java.util.List;
import java.util.Map;

public class DremioJobResultsResponse {
  private long rowCount;
  private List<Map<String, Object>> schema;
  private List<Map<String, Object>> rows;

  public long getRowCount() {
    return rowCount;
  }

  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  public List<Map<String, Object>> getSchema() {
    return schema;
  }

  public void setSchema(List<Map<String, Object>> schema) {
    this.schema = schema;
  }

  public List<Map<String, Object>> getRows() {
    return rows;
  }

  public void setRows(List<Map<String, Object>> rows) {
    this.rows = rows;
  }
}
