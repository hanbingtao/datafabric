package com.datafabric.model;

import java.time.Instant;

public class JobRecord {
  private String id;
  private String sql;
  private JobStatus status;
  private Instant createdAt;
  private Instant startedAt;
  private Instant completedAt;
  private Long rowCount;
  private String resultPath;
  private String errorMessage;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Instant createdAt) {
    this.createdAt = createdAt;
  }

  public Instant getStartedAt() {
    return startedAt;
  }

  public void setStartedAt(Instant startedAt) {
    this.startedAt = startedAt;
  }

  public Instant getCompletedAt() {
    return completedAt;
  }

  public void setCompletedAt(Instant completedAt) {
    this.completedAt = completedAt;
  }

  public Long getRowCount() {
    return rowCount;
  }

  public void setRowCount(Long rowCount) {
    this.rowCount = rowCount;
  }

  public String getResultPath() {
    return resultPath;
  }

  public void setResultPath(String resultPath) {
    this.resultPath = resultPath;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }
}
