package com.datafabric.dto;

import java.util.List;

public class ScriptSummaryResponse {
  private String id;
  private String name;
  private String content;
  private String context;
  private List<String> contextList;
  private Long createdAt;
  private Long modifiedAt;
  private String createdBy;
  private String jobIds;
  private java.util.List<String> jobResultUrls;
  private java.util.List<String> permissions;

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

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public String getContext() {
    return context;
  }

  public void setContext(String context) {
    this.context = context;
  }

  public List<String> getContextList() {
    return contextList;
  }

  public void setContextList(List<String> contextList) {
    this.contextList = contextList;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Long createdAt) {
    this.createdAt = createdAt;
  }

  public Long getModifiedAt() {
    return modifiedAt;
  }

  public void setModifiedAt(Long modifiedAt) {
    this.modifiedAt = modifiedAt;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public String getJobIds() {
    return jobIds;
  }

  public void setJobIds(String jobIds) {
    this.jobIds = jobIds;
  }

  public java.util.List<String> getJobResultUrls() {
    return jobResultUrls;
  }

  public void setJobResultUrls(java.util.List<String> jobResultUrls) {
    this.jobResultUrls = jobResultUrls;
  }

  public java.util.List<String> getPermissions() {
    return permissions;
  }

  public void setPermissions(java.util.List<String> permissions) {
    this.permissions = permissions;
  }
}
