package com.datafabric.controller;

import com.datafabric.service.JobListingCompatibilityService;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/apiv2/queryProfile")
public class JobProfileCompatibilityController {
  private final JobListingCompatibilityService jobListingCompatibilityService;

  public JobProfileCompatibilityController(
      JobListingCompatibilityService jobListingCompatibilityService) {
    this.jobListingCompatibilityService = jobListingCompatibilityService;
  }

  @GetMapping("/{jobId}/JobProfile")
  public List<Map<String, Object>> getJobProfile(
      @PathVariable String jobId, @RequestParam(defaultValue = "1") int attempt) {
    return jobListingCompatibilityService.getJobProfile(jobId, attempt);
  }

  @GetMapping("/{jobId}/JobProfile/OperatorDetails")
  public Map<String, Object> getOperatorDetails(
      @PathVariable String jobId,
      @RequestParam(defaultValue = "1") int attempt,
      @RequestParam(defaultValue = "0") int phaseId,
      @RequestParam(defaultValue = "0") int operatorId) {
    return jobListingCompatibilityService.getOperatorDetails(jobId, attempt, phaseId, operatorId);
  }
}
