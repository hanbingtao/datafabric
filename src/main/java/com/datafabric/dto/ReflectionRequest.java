package com.datafabric.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class ReflectionRequest {
  @NotBlank private String name;
  @NotBlank private String sql;
  @Min(30) private long refreshIntervalSeconds = 300;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public long getRefreshIntervalSeconds() {
    return refreshIntervalSeconds;
  }

  public void setRefreshIntervalSeconds(long refreshIntervalSeconds) {
    this.refreshIntervalSeconds = refreshIntervalSeconds;
  }
}
