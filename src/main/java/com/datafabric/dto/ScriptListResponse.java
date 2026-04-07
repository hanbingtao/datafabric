package com.datafabric.dto;

import java.util.List;

public class ScriptListResponse {
  private List<ScriptSummaryResponse> data;
  private int total;

  public ScriptListResponse() {}

  public ScriptListResponse(List<ScriptSummaryResponse> data, int total) {
    this.data = data;
    this.total = total;
  }

  public List<ScriptSummaryResponse> getData() {
    return data;
  }

  public void setData(List<ScriptSummaryResponse> data) {
    this.data = data;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }
}
