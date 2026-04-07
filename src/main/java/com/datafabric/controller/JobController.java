package com.datafabric.controller;

import com.datafabric.dto.JobDataResponse;
import com.datafabric.model.JobRecord;
import com.datafabric.service.JobService;
import java.io.IOException;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/api/v2/job", "/apiv2/job"})
public class JobController {
  private final JobService jobService;

  public JobController(JobService jobService) {
    this.jobService = jobService;
  }

  @GetMapping
  public List<JobRecord> listJobs() {
    return jobService.listJobs();
  }

  @GetMapping("/{jobId}")
  public JobRecord getJob(@PathVariable String jobId) {
    return jobService.getJob(jobId);
  }

  @GetMapping("/{jobId}/details")
  public Object getJobDetails(@PathVariable String jobId) {
    return jobService.getJobDetails(jobId);
  }

  @GetMapping("/{jobId}/summary")
  public Object getJobSummary(
      @PathVariable String jobId, @RequestParam(defaultValue = "200") int maxSqlLength) {
    return jobService.getJobSummary(jobId, maxSqlLength);
  }

  @GetMapping("/{jobId}/data")
  public JobDataResponse getJobData(
      @PathVariable String jobId,
      @RequestParam(defaultValue = "0") int offset,
      @RequestParam(defaultValue = "100") int limit)
      throws IOException {
    return jobService.getJobData(jobId, offset, limit);
  }

  @PostMapping("/{jobId}/cancel")
  public Object cancelJob(@PathVariable String jobId) {
    return jobService.cancelJob(jobId);
  }
}
