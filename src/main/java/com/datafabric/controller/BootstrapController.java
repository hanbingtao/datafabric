package com.datafabric.controller;

import com.datafabric.dto.JobAndUserStatsResponse;
import com.datafabric.dto.SettingsRequest;
import com.datafabric.dto.SettingsWrapperResponse;
import com.datafabric.dto.SourceListResponse;
import com.datafabric.dto.UserLoginRequest;
import com.datafabric.dto.UserLoginSessionResponse;
import com.datafabric.service.BootstrapService;
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

  public BootstrapController(BootstrapService bootstrapService) {
    this.bootstrapService = bootstrapService;
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
