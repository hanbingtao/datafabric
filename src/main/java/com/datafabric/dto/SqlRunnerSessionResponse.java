package com.datafabric.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
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

  @JsonAlias("activeScriptId")
  public void setCurrentScriptId(String currentScriptId) {
    this.currentScriptId = currentScriptId;
  }
}
