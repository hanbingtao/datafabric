package com.datafabric.dto;

public class QueryDetailsResponse {
  private String id;

  public QueryDetailsResponse() {}

  public QueryDetailsResponse(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
