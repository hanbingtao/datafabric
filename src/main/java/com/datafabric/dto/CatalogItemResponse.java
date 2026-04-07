package com.datafabric.dto;

import java.util.List;

public class CatalogItemResponse {
  private String id;
  private List<String> path;
  private String type;
  private String containerType;
  private String datasetType;
  private String tag;
  private Long createdAt;
  private String sourceChangeState;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<String> getPath() {
    return path;
  }

  public void setPath(List<String> path) {
    this.path = path;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getContainerType() {
    return containerType;
  }

  public void setContainerType(String containerType) {
    this.containerType = containerType;
  }

  public String getDatasetType() {
    return datasetType;
  }

  public void setDatasetType(String datasetType) {
    this.datasetType = datasetType;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Long createdAt) {
    this.createdAt = createdAt;
  }

  public String getSourceChangeState() {
    return sourceChangeState;
  }

  public void setSourceChangeState(String sourceChangeState) {
    this.sourceChangeState = sourceChangeState;
  }
}
