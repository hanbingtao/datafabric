package com.datafabric.controller;

import com.datafabric.dto.JobAndUserStatsResponse;
import com.datafabric.dto.QueryDetailsResponse;
import com.datafabric.dto.SettingsRequest;
import com.datafabric.dto.SettingsWrapperResponse;
import com.datafabric.dto.SourceListResponse;
import com.datafabric.dto.SqlRequest;
import com.datafabric.dto.UserLoginRequest;
import com.datafabric.dto.UserLoginSessionResponse;
import com.datafabric.service.BootstrapService;
import com.datafabric.service.JobService;
import java.util.List;
import java.util.Map;
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
@RequestMapping("/apiv2")
public class BootstrapController {
  private final BootstrapService bootstrapService;
  private final JobService jobService;

  public BootstrapController(BootstrapService bootstrapService, JobService jobService) {
    this.bootstrapService = bootstrapService;
    this.jobService = jobService;
  }

  @GetMapping({"/login", "/login/"})
  public boolean checkLogin() {
    return true;
  }

  @PostMapping({"/login", "/login/"})
  public UserLoginSessionResponse login(@RequestBody(required = false) UserLoginRequest request) {
    return bootstrapService.login(request);
  }

  @DeleteMapping({"/login", "/login/"})
  public void logout() {}

  @GetMapping({"/sources", "/sources/"})
  public SourceListResponse listSources(
      @RequestParam(defaultValue = "false") boolean includeDatasetCount) {
    return bootstrapService.listSources(includeDatasetCount);
  }

  @PostMapping({"/sources/isMetadataImpacting", "/sources/isMetadataImpacting/"})
  public Map<String, Object> isMetadataImpacting(
      @RequestBody(required = false) Map<String, Object> body) {
    return bootstrapService.isMetadataImpacting(body == null ? Map.of() : body);
  }

  @GetMapping({"/home/{homeName}", "/home/{homeName}/"})
  public Map<String, Object> getHome(
      @PathVariable String homeName,
      @RequestParam(defaultValue = "true") boolean includeContents) {
    return bootstrapService.getHome(homeName, includeContents);
  }

  @GetMapping({"/source/{sourceName}", "/source/{sourceName}/"})
  public Map<String, Object> getSource(
      @PathVariable String sourceName,
      @RequestParam(defaultValue = "true") boolean includeContents) {
    return bootstrapService.getSource(sourceName, includeContents);
  }

  @PutMapping({"/source/{sourceName}", "/source/{sourceName}/"})
  public Map<String, Object> saveSource(
      @PathVariable String sourceName,
      @RequestBody(required = false) Map<String, Object> body) {
    return bootstrapService.saveSource(sourceName, body == null ? Map.of() : body);
  }

  @GetMapping({"/space/{spaceName}", "/space/{spaceName}/"})
  public Map<String, Object> getSpace(
      @PathVariable String spaceName,
      @RequestParam(defaultValue = "true") boolean includeContents) {
    return bootstrapService.getSpace(spaceName, includeContents);
  }

  @GetMapping({"/resourcetree", "/resourcetree/"})
  public Map<String, Object> getResourceTree(
      @RequestParam(defaultValue = "false") boolean showDatasets,
      @RequestParam(defaultValue = "false") boolean showSources,
      @RequestParam(defaultValue = "false") boolean showSpaces,
      @RequestParam(defaultValue = "false") boolean showHomes) {
    return bootstrapService.getResourceTree(showDatasets, showSources, showSpaces, showHomes);
  }

  @GetMapping({"/resourcetree/{rootPath}", "/resourcetree/{rootPath}/"})
  public Map<String, Object> getResourceTreePath(
      @PathVariable String rootPath,
      @RequestParam(defaultValue = "false") boolean showDatasets,
      @RequestParam(defaultValue = "false") boolean showSources,
      @RequestParam(defaultValue = "false") boolean showSpaces,
      @RequestParam(defaultValue = "false") boolean showHomes) {
    return bootstrapService.getResourceTreePath(rootPath, showDatasets, showSources, showSpaces, showHomes);
  }

  @GetMapping({"/sql/functions", "/sql/functions/"})
  public Map<String, Object> listSqlFunctions() {
    return bootstrapService.listSqlFunctions();
  }

  // Jobs 列表端点
  @GetMapping({"/jobs", "/jobs/"})
  public Map<String, Object> listJobs(
      @RequestParam(defaultValue = "0") int offset,
      @RequestParam(defaultValue = "100") int limit,
      @RequestParam(required = false) String filter,
      @RequestParam(required = false) String sort,
      @RequestParam(required = false) String order) {
    return bootstrapService.listJobs(offset, limit, filter, sort, order);
  }

  // SQL 执行端点
  @PostMapping(value = {"/sql", "/sql/"}, consumes = "application/json")
  public Map<String, Object> runSql(@RequestBody(required = false) SqlRequest request) {
    if (request == null || request.getSql() == null || request.getSql().isBlank()) {
      return Map.of("jobId", Map.of("id", ""), "status", "FAILED", "message", "SQL is required");
    }
    String jobId = jobService.submitSql(request.getSql());
    return Map.of("jobId", Map.of("id", jobId), "status", "SUBMITTED");
  }

  @GetMapping({"/trees/-", "/trees/-/"})
  public Map<String, Object> getDefaultTreeReference() {
    return bootstrapService.getDefaultTreeReference();
  }

  @GetMapping({"/trees", "/trees/"})
  public Map<String, Object> listTreeReferences() {
    return bootstrapService.listTreeReferences();
  }

  @GetMapping({"/server_status", "/server_status/"})
  public Map<String, Object> serverStatus() {
    return bootstrapService.getServerStatus();
  }

  @GetMapping({"/system/nodes", "/system/nodes/"})
  public Map<String, Object> systemNodes() {
    return bootstrapService.getSystemNodes();
  }

  @GetMapping({"/stats/jobsandusers", "/stats/jobsandusers/"})
  public JobAndUserStatsResponse jobsAndUsers(
      @RequestParam(defaultValue = "7") int numDaysBack,
      @RequestParam(defaultValue = "false") boolean detailedStats) {
    return bootstrapService.getJobsAndUsers(numDaysBack, detailedStats);
  }

  @GetMapping({"/stats/user", "/stats/user/"})
  public List<Map<String, Object>> userStats(
      @RequestParam(defaultValue = "0") long start, @RequestParam(defaultValue = "0") long end) {
    return bootstrapService.getUserStats(start, end);
  }

  @GetMapping({"/cluster/jobstats", "/cluster/jobstats/"})
  public List<Map<String, Object>> jobStats(
      @RequestParam(defaultValue = "0") long start, @RequestParam(defaultValue = "0") long end) {
    return bootstrapService.getJobStats(start, end);
  }

  @GetMapping({"/settings/{supportKey}", "/settings/{supportKey}/"})
  public Map<String, Object> supportFlag(@PathVariable String supportKey) {
    return bootstrapService.getSupportFlag(supportKey);
  }

  @PostMapping({"/settings", "/settings/"})
  public SettingsWrapperResponse listSettings(@RequestBody(required = false) SettingsRequest request) {
    return bootstrapService.listSettings(request);
  }

  @PutMapping({"/settings/{supportKey}", "/settings/{supportKey}/"})
  public Map<String, Object> saveSupportFlag(@PathVariable String supportKey, @RequestBody Map<String, Object> body) {
    return body == null || body.isEmpty() ? bootstrapService.getSupportFlag(supportKey) : body;
  }

}
