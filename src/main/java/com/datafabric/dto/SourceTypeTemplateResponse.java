package com.datafabric.dto;

import java.util.List;
import java.util.Map;

public class SourceTypeTemplateResponse {
  private String sourceType;
  private String label;
  private String icon;
  private boolean externalQueryAllowed;
  private boolean previewEngineRequired;
  private List<Map<String, Object>> elements;
  private Map<String, Object> uiConfig;

  public String getSourceType() {
    return sourceType;
  }

  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getIcon() {
    return icon;
  }

  public void setIcon(String icon) {
    this.icon = icon;
  }

  public boolean isExternalQueryAllowed() {
    return externalQueryAllowed;
  }

  public void setExternalQueryAllowed(boolean externalQueryAllowed) {
    this.externalQueryAllowed = externalQueryAllowed;
  }

  public boolean isPreviewEngineRequired() {
    return previewEngineRequired;
  }

  public void setPreviewEngineRequired(boolean previewEngineRequired) {
    this.previewEngineRequired = previewEngineRequired;
  }

  public List<Map<String, Object>> getElements() {
    return elements;
  }

  public void setElements(List<Map<String, Object>> elements) {
    this.elements = elements;
  }

  public Map<String, Object> getUiConfig() {
    return uiConfig;
  }

  public void setUiConfig(Map<String, Object> uiConfig) {
    this.uiConfig = uiConfig;
  }
}
