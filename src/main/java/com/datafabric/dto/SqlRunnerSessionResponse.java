package com.datafabric.dto;

import java.util.ArrayList;
import java.util.List;

public class SqlRunnerSessionResponse {
  private List<String> scriptIds = new ArrayList<>();
  private String currentScriptId;

  public List<String> getScriptIds() {
    return scriptIds;
  }

  public void setScriptIds(List<String> scriptIds) {
    this.scriptIds = scriptIds;
  }

  public String getCurrentScriptId() {
    return currentScriptId;
  }

  public void setCurrentScriptId(String currentScriptId) {
    this.currentScriptId = currentScriptId;
  }
}
