package com.datafabric.dto;

import java.util.List;

public class CollaborationTagResponse {
  private List<String> tags;
  private String version;

  public CollaborationTagResponse() {}

  public CollaborationTagResponse(List<String> tags, String version) {
    this.tags = tags;
    this.version = version;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
