package com.datafabric.dto;

import java.util.List;

public class ApiV3ListResponse<T> {
  private List<T> data;

  public ApiV3ListResponse() {}

  public ApiV3ListResponse(List<T> data) {
    this.data = data;
  }

  public List<T> getData() {
    return data;
  }

  public void setData(List<T> data) {
    this.data = data;
  }
}
