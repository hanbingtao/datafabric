package com.datafabric.controller;

import com.datafabric.dto.ScriptListResponse;
import com.datafabric.dto.ScriptRequest;
import com.datafabric.dto.ScriptSummaryResponse;
import com.datafabric.dto.SqlRunnerSessionResponse;
import com.datafabric.service.SqlRunnerCompatibilityService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class SqlRunnerCompatibilityController {
  private final SqlRunnerCompatibilityService sqlRunnerCompatibilityService;

  public SqlRunnerCompatibilityController(SqlRunnerCompatibilityService sqlRunnerCompatibilityService) {
    this.sqlRunnerCompatibilityService = sqlRunnerCompatibilityService;
  }

  @GetMapping({"/apiv2/scripts", "/apiv2/scripts/", "/api/v3/scripts", "/api/v3/scripts/"})
  public ScriptListResponse listScripts(
      @RequestParam(defaultValue = "1000") int maxResults,
      @RequestParam(required = false) String search,
      @RequestParam(required = false) String createdBy) {
    return sqlRunnerCompatibilityService.listScripts(maxResults, search, createdBy);
  }

  @PostMapping({"/apiv2/scripts", "/apiv2/scripts/", "/api/v3/scripts", "/api/v3/scripts/"})
  public ScriptSummaryResponse createScript(@RequestBody(required = false) ScriptRequest request) {
    return sqlRunnerCompatibilityService.createScript(request);
  }

  @GetMapping({
    "/apiv2/scripts/{id}",
    "/apiv2/scripts/{id}/",
    "/api/v3/scripts/{id}",
    "/api/v3/scripts/{id}/"
  })
  public ScriptSummaryResponse getScript(@PathVariable String id) {
    return sqlRunnerCompatibilityService.getScript(id);
  }

  @PutMapping({
    "/apiv2/scripts/{id}",
    "/apiv2/scripts/{id}/",
    "/api/v3/scripts/{id}",
    "/api/v3/scripts/{id}/"
  })
  public ScriptSummaryResponse updateScript(
      @PathVariable String id, @RequestBody(required = false) ScriptRequest request) {
    return sqlRunnerCompatibilityService.updateScript(id, request);
  }

  @PutMapping({
    "/apiv2/scripts/{id}/update_context",
    "/apiv2/scripts/{id}/update_context/",
    "/api/v3/scripts/{id}/update_context",
    "/api/v3/scripts/{id}/update_context/"
  })
  public ScriptSummaryResponse updateScriptContext(
      @PathVariable String id, @RequestBody(required = false) String sessionId) {
    return sqlRunnerCompatibilityService.updateContext(id, sessionId);
  }

  @DeleteMapping({
    "/apiv2/scripts/{id}",
    "/apiv2/scripts/{id}/",
    "/api/v3/scripts/{id}",
    "/api/v3/scripts/{id}/"
  })
  public ResponseEntity<Void> deleteScript(@PathVariable String id) {
    sqlRunnerCompatibilityService.deleteScript(id);
    return ResponseEntity.noContent().build();
  }

  @GetMapping({"/sql-runner/session", "/sql-runner/session/"})
  public SqlRunnerSessionResponse getSession() {
    return sqlRunnerCompatibilityService.getSession();
  }

  @PutMapping({"/sql-runner/session", "/sql-runner/session/"})
  public SqlRunnerSessionResponse updateSession(
      @RequestBody(required = false) SqlRunnerSessionResponse request) {
    return sqlRunnerCompatibilityService.updateSession(request);
  }

  @PutMapping({"/sql-runner/session/tabs/{scriptId}", "/sql-runner/session/tabs/{scriptId}/"})
  public SqlRunnerSessionResponse addTab(@PathVariable String scriptId) {
    return sqlRunnerCompatibilityService.addTab(scriptId);
  }

  @DeleteMapping({"/sql-runner/session/tabs/{scriptId}", "/sql-runner/session/tabs/{scriptId}/"})
  public ResponseEntity<Void> deleteTab(@PathVariable String scriptId) {
    sqlRunnerCompatibilityService.deleteTab(scriptId);
    return ResponseEntity.noContent().build();
  }

}
