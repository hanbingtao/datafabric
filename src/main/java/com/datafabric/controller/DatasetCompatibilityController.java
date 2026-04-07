package com.datafabric.controller;

import com.datafabric.dto.CreateFromSqlRequest;
import com.datafabric.service.DatasetCompatibilityService;
import jakarta.servlet.http.HttpServletRequest;
import java.sql.SQLException;
import java.util.Map;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/apiv2/datasets")
public class DatasetCompatibilityController {
  private final DatasetCompatibilityService datasetCompatibilityService;

  public DatasetCompatibilityController(DatasetCompatibilityService datasetCompatibilityService) {
    this.datasetCompatibilityService = datasetCompatibilityService;
  }

  @PostMapping("/new_untitled_sql")
  public Map<String, Object> newUntitledSql(
      @RequestParam(required = false) String newVersion,
      @RequestParam(required = false) String sessionId,
      @RequestParam(defaultValue = "false") boolean triggerJob,
      @RequestBody(required = false) CreateFromSqlRequest request) {
    return datasetCompatibilityService.createUntitledSql(
        newVersion, sessionId, false, triggerJob, request);
  }

  @PostMapping("/new_tmp_untitled_sql")
  public Map<String, Object> newTmpUntitledSql(
      @RequestParam(required = false) String newVersion,
      @RequestParam(required = false) String sessionId,
      @RequestParam(defaultValue = "false") boolean triggerJob,
      @RequestBody(required = false) CreateFromSqlRequest request) {
    return datasetCompatibilityService.createUntitledSql(
        newVersion, sessionId, true, triggerJob, request);
  }

  @PostMapping("/new_untitled_sql_and_run")
  public Map<String, Object> newUntitledSqlAndRun(
      @RequestParam(required = false) String newVersion,
      @RequestParam(required = false) String sessionId,
      @RequestBody(required = false) CreateFromSqlRequest request) {
    return datasetCompatibilityService.createUntitledSql(
        newVersion, sessionId, false, true, request);
  }

  @PostMapping("/new_tmp_untitled_sql_and_run")
  public Map<String, Object> newTmpUntitledSqlAndRun(
      @RequestParam(required = false) String newVersion,
      @RequestParam(required = false) String sessionId,
      @RequestBody(required = false) CreateFromSqlRequest request) {
    return datasetCompatibilityService.createUntitledSql(
        newVersion, sessionId, true, true, request);
  }

  @GetMapping({"/summary/{path:.+}", "/summary/**"})
  public Map<String, Object> getDatasetSummary(
      @PathVariable(required = false) String path, HttpServletRequest request) throws SQLException {
    if (path == null || path.isBlank()) {
      String uri = request.getRequestURI();
      int marker = uri.indexOf("/apiv2/datasets/summary/");
      path = marker >= 0 ? uri.substring(marker + "/apiv2/datasets/summary/".length()) : "";
    }
    return datasetCompatibilityService.getDatasetSummary(path);
  }

}
