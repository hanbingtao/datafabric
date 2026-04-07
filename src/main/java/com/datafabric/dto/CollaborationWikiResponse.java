package com.datafabric.dto;

public class CollaborationWikiResponse {
  private String text;
  private Long version;

  public CollaborationWikiResponse() {}

  public CollaborationWikiResponse(String text, Long version) {
    this.text = text;
    this.version = version;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }
}
