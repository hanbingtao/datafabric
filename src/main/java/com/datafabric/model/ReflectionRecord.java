package com.datafabric.model;

import java.time.Instant;

public class ReflectionRecord {
  private String id;
  private String name;
  private String sql;
  private ReflectionStatus status;
  private Instant createdAt;
  private Instant lastRefreshAt;
  private Instant nextRefreshAt;
  private long refreshIntervalSeconds;
  private String materializationPath;
  private String errorMessage;

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

  public long getRefreshIntervalSeconds() {
    return refreshIntervalSeconds;
  }

  public void setRefreshIntervalSeconds(long refreshIntervalSeconds) {
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
}
