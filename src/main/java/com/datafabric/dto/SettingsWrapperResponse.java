package com.datafabric.dto;

import java.util.List;

public class SettingsWrapperResponse {
  private List<SettingResponse> settings;

  public SettingsWrapperResponse() {}

  public SettingsWrapperResponse(List<SettingResponse> settings) {
    this.settings = settings;
  }

  public List<SettingResponse> getSettings() {
    return settings;
  }

  public void setSettings(List<SettingResponse> settings) {
    this.settings = settings;
  }
}
