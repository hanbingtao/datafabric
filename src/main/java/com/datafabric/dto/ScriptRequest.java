package com.datafabric.dto;

import java.util.List;

public class ScriptRequest {
  private String name;
  private String content;
  private String description;
  private List<String> context;
  private List<String> jobIds;

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
}
