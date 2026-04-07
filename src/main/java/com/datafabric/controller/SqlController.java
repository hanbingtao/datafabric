package com.datafabric.controller;

import com.datafabric.dto.QueryDetailsResponse;
import com.datafabric.dto.SqlRequest;
import com.datafabric.service.JobService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v3/sql")
public class SqlController {
  private final JobService jobService;

  public SqlController(JobService jobService) {
    this.jobService = jobService;
  }

  @PostMapping
  public QueryDetailsResponse runQuery(@Valid @RequestBody SqlRequest request) {
    return new QueryDetailsResponse(jobService.submitSql(request.getSql()));
  }
}
