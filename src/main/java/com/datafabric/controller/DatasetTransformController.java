package com.datafabric.controller;

import com.datafabric.dto.TransformRequest;
import com.datafabric.service.DatasetTransformService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/apiv2/datasets", "/api/v2/datasets"})
public class DatasetTransformController {
  private final DatasetTransformService transformService;

  public DatasetTransformController(DatasetTransformService transformService) {
    this.transformService = transformService;
  }

  /**
   * Get the initial transformation options for a dataset. This returns the available transformation types (filter, sort, etc.)
   * and the current schema of the dataset.
   */
  @GetMapping("/{path:.+}/version/{version}/initial")
  public Map<String, Object> getInitialTransform(
      @PathVariable String path,
      @PathVariable String version,
      @RequestParam(required = false) String sessionId) {
    return transformService.getInitialTransform(path, version, sessionId);
  }

  /**
   * Apply a transformation (filter, sort, join, etc.) and return a preview of the result.
   */
  @PostMapping("/{path:.+}/version/{version}/transform")
  public Map<String, Object> transform(
      @PathVariable String path,
      @PathVariable String version,
      @RequestParam(required = false) String sessionId,
      @RequestBody(required = false) TransformRequest request) {
    return transformService.transform(path, version, sessionId, request);
  }

  /**
   * Get preview of data after applying transformations.
   */
  @GetMapping("/{path:.+}/version/{version}/preview")
  public Map<String, Object> preview(
      @PathVariable String path,
      @PathVariable String version,
      @RequestParam(required = false) String sessionId,
      @RequestParam(required = false) Integer limit) {
    return transformService.preview(path, version, sessionId, limit != null ? limit : 100);
  }

  /**
   * Get histogram/counts for a column to help with filter suggestions.
   */
  @GetMapping("/{path:.+}/version/{version}/histogram/{column}")
  public Map<String, Object> getHistogram(
      @PathVariable String path,
      @PathVariable String version,
      @PathVariable String column,
      @RequestParam(required = false) Integer numBins) {
    return transformService.getHistogram(path, version, column, numBins != null ? numBins : 10);
  }

  /**
   * Get sample values for a column (for autocomplete).
   */
  @GetMapping("/{path:.+}/version/{version}/samples/{column}")
  public Map<String, Object> getSampleValues(
      @PathVariable String path,
      @PathVariable String version,
      @PathVariable String column,
      @RequestParam(defaultValue = "10") int limit) {
    return transformService.getSampleValues(path, version, column, limit);
  }
}
