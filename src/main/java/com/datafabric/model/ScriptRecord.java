package com.datafabric.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ScriptRecord {
  private String id;
  private String name;
  private String content;
  private String description;
  private List<String> context = new ArrayList<>();
  private List<String> jobIds = new ArrayList<>();
  private List<String> jobResultUrls = new ArrayList<>();
  private List<String> permissions = new ArrayList<>();
  private String createdBy;
  private Instant createdAt;
  private Instant modifiedAt;

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

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<String> getContext() {
    return context;
  }

  public void setContext(List<String> context) {
    this.context = context;
  }

  public List<String> getJobIds() {
    return jobIds;
  }

  public void setJobIds(List<String> jobIds) {
    this.jobIds = jobIds;
  }

  public List<String> getJobResultUrls() {
    return jobResultUrls;
  }

  public void setJobResultUrls(List<String> jobResultUrls) {
    this.jobResultUrls = jobResultUrls;
  }

  public List<String> getPermissions() {
    return permissions;
  }

  public void setPermissions(List<String> permissions) {
    this.permissions = permissions;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Instant createdAt) {
    this.createdAt = createdAt;
  }

  public Instant getModifiedAt() {
    return modifiedAt;
  }

  public void setModifiedAt(Instant modifiedAt) {
    this.modifiedAt = modifiedAt;
  }
}
