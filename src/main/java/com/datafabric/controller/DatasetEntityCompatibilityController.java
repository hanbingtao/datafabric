package com.datafabric.controller;

import com.datafabric.service.DatasetCompatibilityService;
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

  @GetMapping({"/{path:.+}/version/{version}/preview", "/{path:.+}/version/{version}/preview/"})
  public Map<String, Object> getPreview(
      @PathVariable String path,
      @PathVariable String version,
      @RequestParam(required = false) String sessionId) throws SQLException, IOException {
    return datasetCompatibilityService.getDatasetPreview(path, version, sessionId);
  }

  @GetMapping({"/{path:.+}/version/{version}/review", "/{path:.+}/version/{version}/review/"})
  public Map<String, Object> getReview(
      @PathVariable String path,
      @PathVariable String version,
      @RequestParam(required = false) String sessionId) throws SQLException, IOException {
    return datasetCompatibilityService.getDatasetPreview(path, version, sessionId);
  }
}
