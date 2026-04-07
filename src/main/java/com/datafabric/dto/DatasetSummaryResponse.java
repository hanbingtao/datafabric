package com.datafabric.dto;

import java.util.List;

public class DatasetSummaryResponse {
  private String tableName;
  private List<ColumnInfo> columns;

  public DatasetSummaryResponse() {}

  public DatasetSummaryResponse(String tableName, List<ColumnInfo> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<ColumnInfo> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnInfo> columns) {
    this.columns = columns;
  }

  public record ColumnInfo(String name, String type, int size, boolean nullable) {}
}
