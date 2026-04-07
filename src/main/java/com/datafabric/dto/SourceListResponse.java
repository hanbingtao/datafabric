package com.datafabric.dto;

import java.util.List;
import java.util.Map;

public class SourceListResponse {
  private List<SourceInfo> sources;

  public SourceListResponse() {}

  public SourceListResponse(List<SourceInfo> sources) {
    this.sources = sources;
  }

  public List<SourceInfo> getSources() {
    return sources;
  }

  public void setSources(List<SourceInfo> sources) {
    this.sources = sources;
  }

  public static class SourceInfo {
    private String type;
    private String name;
    private long ctime;
    private String id;
    private String tag;
    private String resourcePath;
    private List<String> fullPathList;
    private Map<String, String> links;
    private Map<String, Object> state;
    private Map<String, Object> config;
    private Map<String, Object> metadataPolicy;
    private Integer numberOfDatasets;

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public long getCtime() {
      return ctime;
    }

    public void setCtime(long ctime) {
      this.ctime = ctime;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getTag() {
      return tag;
    }

    public void setTag(String tag) {
      this.tag = tag;
    }

    public String getResourcePath() {
      return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
      this.resourcePath = resourcePath;
    }

    public List<String> getFullPathList() {
      return fullPathList;
    }

    public void setFullPathList(List<String> fullPathList) {
      this.fullPathList = fullPathList;
    }

    public Map<String, String> getLinks() {
      return links;
    }

    public void setLinks(Map<String, String> links) {
      this.links = links;
    }

    public Map<String, Object> getState() {
      return state;
    }

    public void setState(Map<String, Object> state) {
      this.state = state;
    }

    public Map<String, Object> getConfig() {
      return config;
    }

    public void setConfig(Map<String, Object> config) {
      this.config = config;
    }

    public Map<String, Object> getMetadataPolicy() {
      return metadataPolicy;
    }

    public void setMetadataPolicy(Map<String, Object> metadataPolicy) {
      this.metadataPolicy = metadataPolicy;
    }

    public Integer getNumberOfDatasets() {
      return numberOfDatasets;
    }

    public void setNumberOfDatasets(Integer numberOfDatasets) {
      this.numberOfDatasets = numberOfDatasets;
    }
  }
}
