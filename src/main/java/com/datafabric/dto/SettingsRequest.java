package com.datafabric.dto;

import java.util.Set;

public class SettingsRequest {
  private Set<String> requiredSettings;
  private boolean includeSetSettings;

  public Set<String> getRequiredSettings() {
    return requiredSettings;
  }

  public void setRequiredSettings(Set<String> requiredSettings) {
    this.requiredSettings = requiredSettings;
  }

  public boolean isIncludeSetSettings() {
    return includeSetSettings;
  }

  public void setIncludeSetSettings(boolean includeSetSettings) {
    this.includeSetSettings = includeSetSettings;
  }
}
