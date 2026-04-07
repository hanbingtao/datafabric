package com.datafabric.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class ReflectionRecord {
  private String id;
  private String name;
  private String sql;
  private ReflectionStatus status;
  private Instant createdAt;
  private Instant lastRefreshAt;
  private Instant nextRefreshAt;
  private Long refreshIntervalSeconds;
  private String materializationPath;
  private String errorMessage;
  private List<String> columns;
  private List<Map<String, Object>> rows;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

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

  public ReflectionStatus getStatus() {
    return status;
  }

  public void setStatus(ReflectionStatus status) {
    this.status = status;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Instant createdAt) {
    this.createdAt = createdAt;
  }

  public Instant getLastRefreshAt() {
    return lastRefreshAt;
  }

  public void setLastRefreshAt(Instant lastRefreshAt) {
    this.lastRefreshAt = lastRefreshAt;
  }

  public Instant getNextRefreshAt() {
    return nextRefreshAt;
  }

  public void setNextRefreshAt(Instant nextRefreshAt) {
    this.nextRefreshAt = nextRefreshAt;
  }

  public Long getRefreshIntervalSeconds() {
    return refreshIntervalSeconds;
  }

  public void setRefreshIntervalSeconds(Long refreshIntervalSeconds) {
    this.refreshIntervalSeconds = refreshIntervalSeconds;
  }

  public String getMaterializationPath() {
    return materializationPath;
  }

  public void setMaterializationPath(String materializationPath) {
    this.materializationPath = materializationPath;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

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
}
