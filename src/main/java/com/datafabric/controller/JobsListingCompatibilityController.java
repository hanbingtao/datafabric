package com.datafabric.controller;

import com.datafabric.service.JobListingCompatibilityService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/apiv2/jobs-listing/v1.0")
public class JobsListingCompatibilityController {
  private final JobListingCompatibilityService jobListingCompatibilityService;

  public JobsListingCompatibilityController(
      JobListingCompatibilityService jobListingCompatibilityService) {
    this.jobListingCompatibilityService = jobListingCompatibilityService;
  }

  @GetMapping({"", "/"})
  public Map<String, Object> listJobs(
      @RequestParam(defaultValue = "0") int offset,
      @RequestParam(defaultValue = "100") int limit,
      @RequestParam(required = false) String filter,
      @RequestParam(required = false) String sort,
      @RequestParam(required = false) String order) {
    return jobListingCompatibilityService.listJobs(offset, limit, filter, sort, order);
  }

  @GetMapping("/{jobId}/jobDetails")
  public Map<String, Object> getJobDetails(
      @PathVariable String jobId,
      @RequestParam(defaultValue = "1") int attempt,
      @RequestParam(defaultValue = "1") int detailLevel) {
    return jobListingCompatibilityService.getJobDetails(jobId, attempt, detailLevel);
  }

  @GetMapping("/{jobId}/datasetGraph")
  public Map<String, Object> getDatasetGraph(
      @PathVariable String jobId, @RequestParam(defaultValue = "1") int attempt) {
    return jobListingCompatibilityService.getDatasetGraph(jobId, attempt);
  }
}
