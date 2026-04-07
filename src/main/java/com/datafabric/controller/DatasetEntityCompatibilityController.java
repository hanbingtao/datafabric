package com.datafabric.controller;

import com.datafabric.service.DatasetCompatibilityService;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/apiv2/dataset", "/api/v2/dataset"})
public class DatasetEntityCompatibilityController {
  private final DatasetCompatibilityService datasetCompatibilityService;

  public DatasetEntityCompatibilityController(
      DatasetCompatibilityService datasetCompatibilityService) {
    this.datasetCompatibilityService = datasetCompatibilityService;
  }

  @GetMapping({
    "/{path:.+}/version/{version}/preview",
    "/{path:.+}/version/{version}/preview/",
    "/**/preview",
    "/**/preview/"
  })
  public Map<String, Object> getPreview(
      @PathVariable(required = false) String path,
      @PathVariable(required = false) String version,
      HttpServletRequest request,
      @RequestParam(required = false) String sessionId) throws SQLException, IOException {
    RequestPath requestPath = resolveRequestPath(path, version, request);
    return datasetCompatibilityService.getDatasetPreview(
        requestPath.path(), requestPath.version(), sessionId);
  }

  @GetMapping({
    "/{path:.+}/version/{version}/review",
    "/{path:.+}/version/{version}/review/",
    "/**/review",
    "/**/review/"
  })
  public Map<String, Object> getReview(
      @PathVariable(required = false) String path,
      @PathVariable(required = false) String version,
      HttpServletRequest request,
      @RequestParam(required = false) String sessionId) throws SQLException, IOException {
    RequestPath requestPath = resolveRequestPath(path, version, request);
    return datasetCompatibilityService.getDatasetPreview(
        requestPath.path(), requestPath.version(), sessionId);
  }

  private RequestPath resolveRequestPath(String path, String version, HttpServletRequest request) {
    if (path != null && !path.isBlank() && version != null && !version.isBlank()) {
      return new RequestPath(path, version);
    }

    String uri = request.getRequestURI();
    int marker = uri.indexOf("/dataset/");
    if (marker < 0) {
      throw new IllegalArgumentException("Unsupported dataset preview path: " + uri);
    }

    String remainder = uri.substring(marker + "/dataset/".length());
    String[] segments = remainder.split("/");
    int versionIndex = -1;
    for (int index = 0; index < segments.length; index++) {
      if ("version".equals(segments[index])) {
        versionIndex = index;
        break;
      }
    }
    if (versionIndex <= 0 || versionIndex + 1 >= segments.length) {
      throw new IllegalArgumentException("Unsupported dataset preview path: " + uri);
    }

    String resolvedPath = String.join("/", java.util.Arrays.copyOfRange(segments, 0, versionIndex));
    String resolvedVersion = segments[versionIndex + 1];
    return new RequestPath(resolvedPath, resolvedVersion);
  }

  private record RequestPath(String path, String version) {}
}
