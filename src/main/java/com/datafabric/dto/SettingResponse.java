package com.datafabric.dto;

public class SettingResponse {
  private String id;
  private String type;
  private Object value;

  public SettingResponse() {}

  public SettingResponse(String id, String type, Object value) {
    this.id = id;
    this.type = type;
    this.value = value;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }
}
