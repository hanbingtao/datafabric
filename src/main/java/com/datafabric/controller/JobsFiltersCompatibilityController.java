package com.datafabric.controller;

import com.datafabric.service.JobListingCompatibilityService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/apiv2/jobs/filters")
public class JobsFiltersCompatibilityController {
  private final JobListingCompatibilityService jobListingCompatibilityService;

  public JobsFiltersCompatibilityController(
      JobListingCompatibilityService jobListingCompatibilityService) {
    this.jobListingCompatibilityService = jobListingCompatibilityService;
  }

  @GetMapping("/{tag}")
  public Map<String, Object> getFilterItems(
      @PathVariable String tag,
      @RequestParam(required = false) String filter,
      @RequestParam(defaultValue = "50") int limit) {
    return jobListingCompatibilityService.getFilterItems(tag, filter, limit);
  }
}
