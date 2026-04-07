package com.datafabric.dto;

import java.util.List;
import java.util.Map;

public class JobDataResponse {
  private List<String> columns;
  private List<Map<String, Object>> rows;
  private int offset;
  private int limit;
  private long returned;
  private long total;

  public List<String> getColumns() {
    return columns;
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  public List<Map<String, Object>> getRows() {
    return rows;
  }

  public void setRows(List<Map<String, Object>> rows) {
    this.rows = rows;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public long getReturned() {
    return returned;
  }

  public void setReturned(long returned) {
    this.returned = returned;
  }

  public long getTotal() {
    return total;
  }

  public void setTotal(long total) {
    this.total = total;
  }
}
