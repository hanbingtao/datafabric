package com.datafabric.dto;

import jakarta.validation.constraints.NotBlank;

public class SqlRequest {
  @NotBlank private String sql;

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }
}
