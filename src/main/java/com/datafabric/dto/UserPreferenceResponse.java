package com.datafabric.dto;

import java.util.List;
import java.util.Map;

public class UserPreferenceResponse {
  private String type;
  private List<Map<String, Object>> entities;

  public UserPreferenceResponse() {}

  public UserPreferenceResponse(String type, List<Map<String, Object>> entities) {
    this.type = type;
    this.entities = entities;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public List<Map<String, Object>> getEntities() {
    return entities;
  }

  public void setEntities(List<Map<String, Object>> entities) {
    this.entities = entities;
  }
}
