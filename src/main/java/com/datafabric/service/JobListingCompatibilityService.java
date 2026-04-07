package com.datafabric.service;

import com.datafabric.model.JobRecord;
import com.datafabric.model.JobStatus;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class JobListingCompatibilityService {
  private final JobService jobService;

  public JobListingCompatibilityService(JobService jobService) {
    this.jobService = jobService;
  }

  public Map<String, Object> listJobs(
      int offset, int limit, String filter, String sort, String order) {
    List<Map<String, Object>> jobs =
        jobService.listJobs().stream()
            .skip(Math.max(offset, 0))
            .limit(Math.max(limit, 1))
            .map(this::toJobSummary)
            .toList();

    Map<String, Object> response = new LinkedHashMap<>();
    response.put("jobs", jobs);
    response.put("next", null);
    return response;
  }

  public Map<String, Object> getJobDetails(String jobId, int attempt, int detailLevel) {
    if (jobId == null || jobId.isBlank() || "undefined".equalsIgnoreCase(jobId)) {
      return placeholderJobDetails();
    }

    JobRecord job = jobService.getJob(jobId);
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("jobId", Map.of("id", job.getId()));
    details.put("id", job.getId());
    details.put("jobStatus", job.getStatus().name());
    details.put("state", job.getStatus().name());
    details.put("queryType", "UI_RUN");
    details.put("requestType", "RUN_SQL");
    details.put("queryText", job.getSql());
    details.put("description", shortDescription(job.getSql()));
    details.put("datasetPaths", List.of("Samples.SALES_FACT"));
    details.put("datasetVersion", "000000000000");
    details.put("attemptDetails", List.of(attemptSummary(job)));
    details.put("duration", duration(job));
    details.put("startTime", epoch(job.getStartedAt() != null ? job.getStartedAt() : job.getCreatedAt()));
    details.put("endTime", epoch(job.getCompletedAt()));
    details.put("outputRecords", job.getRowCount() == null ? 0L : job.getRowCount());
    details.put("outputBytes", 0L);
    details.put("inputRecords", 0L);
    details.put("inputBytes", 0L);
    details.put("totalMemory", 0L);
    details.put("cpuUsed", 0L);
    details.put("waitInClient", 0L);
    details.put("rowsScanned", 0L);
    details.put("isComplete", isComplete(job));
    details.put("resultsAvailable", job.getStatus() == JobStatus.COMPLETED);
    details.put("isOutputLimited", false);
    details.put("isProfileUpdateComplete", true);
    details.put("failureInfo", failureInfo(job));
    details.put("cancellationInfo", null);
    details.put("queryUser", "datafabric");
    details.put("wlmQueue", "datafabric");
    details.put("queriedDatasets", List.of(Map.of("datasetPath", List.of("Samples", "SALES_FACT"))));
    details.put("scannedDatasets", List.of());
    details.put("reflections", List.of());
    details.put("reflectionsUsed", List.of());
    details.put("reflectionsMatched", List.of());
    details.put("resultsCacheUsed", false);
    details.put("accelerated", false);
    details.put("isProfileIncomplete", false);
    details.put("totalAttempts", 1);
    details.put("engine", "datafabric");
    details.put("spilled", false);
    return details;
  }

  public Map<String, Object> getDatasetGraph(String jobId, int attempt) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put(
        "datasetGraph",
        List.of(
            Map.of(
                "id", "dataset-" + (jobId == null ? "unknown" : jobId),
                "displayPath", "Samples.SALES_FACT",
                "description", "Samples.SALES_FACT",
                "datasetType", "PHYSICAL_DATASET",
                "parentNodeIdList", List.of())));
    return response;
  }

  public Map<String, Object> getFilterItems(String tag, String filter, int limit) {
    List<Map<String, Object>> items;
    String normalizedTag = tag == null ? "" : tag.toLowerCase();
    if ("users".equals(normalizedTag)) {
      items = List.of(Map.of("id", "datafabric", "label", "datafabric"));
    } else if ("spaces".equals(normalizedTag) || "contains".equals(normalizedTag)) {
      items = List.of(Map.of("id", "Samples", "label", "Samples"));
    } else if ("queryType".equals(normalizedTag)) {
      items = List.of(Map.of("id", "UI_RUN", "label", "UI_RUN"));
    } else {
      items = List.of();
    }
    return Map.of("items", items.stream().limit(Math.max(1, limit)).toList());
  }

  public List<Map<String, Object>> getJobProfile(String jobId, int attempt) {
    JobRecord job = resolveJob(jobId);
    return List.of(
        Map.of(
            "phaseId", 0,
            "phaseName", "SCAN",
            "processTime", duration(job),
            "peakMemory", 0L,
            "recordsProcessed", job.getRowCount() == null ? 0L : job.getRowCount(),
            "numThreads", 1,
            "operatorDataList", List.of()));
  }

  public Map<String, Object> getOperatorDetails(
      String jobId, int attempt, int phaseId, int operatorId) {
    JobRecord job = resolveJob(jobId);
    return Map.of(
        "phaseId", phaseId,
        "operatorId", operatorId,
        "operatorName", "SCREEN",
        "setupTime", 0L,
        "processTime", duration(job),
        "waitTime", 0L,
        "peakMemory", 0L,
        "recordsProcessed", job.getRowCount() == null ? 0L : job.getRowCount(),
        "metricsMap", Map.of());
  }

  private Map<String, Object> toJobSummary(JobRecord job) {
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("id", job.getId());
    summary.put("jobId", Map.of("id", job.getId()));
    summary.put("state", job.getStatus().name());
    summary.put("jobStatus", job.getStatus().name());
    summary.put("queryType", "UI_RUN");
    summary.put("requestType", "RUN_SQL");
    summary.put("description", shortDescription(job.getSql()));
    summary.put("outputRecords", job.getRowCount() == null ? 0L : job.getRowCount());
    summary.put("outputLimited", false);
    summary.put("isComplete", isComplete(job));
    summary.put("startTime", epoch(job.getCreatedAt()));
    if (job.getCompletedAt() != null) {
      summary.put("endTime", epoch(job.getCompletedAt()));
    }
    summary.put("datasetVersion", "000000000000");
    summary.put("failureInfo", failureInfo(job));
    summary.put("cancellationInfo", null);
    summary.put("accelerated", false);
    summary.put("spilled", false);
    return summary;
  }

  private Map<String, Object> attemptSummary(JobRecord job) {
    return Map.of(
        "reason", job.getStatus().name(),
        "result", job.getStatus().name(),
        "profileUrl", "/profiles/" + job.getId(),
        "executionId", job.getId());
  }

  private Map<String, Object> failureInfo(JobRecord job) {
    if (job.getStatus() != JobStatus.FAILED) {
      return Map.of("errors", List.of(), "message", "", "type", "UNKNOWN");
    }
    return Map.of(
        "errors", List.of(Map.of("message", nullToEmpty(job.getErrorMessage()), "type", "ERROR")),
        "message", nullToEmpty(job.getErrorMessage()),
        "type", "ERROR");
  }

  private Map<String, Object> placeholderJobDetails() {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("jobId", Map.of("id", "undefined"));
    details.put("id", "undefined");
    details.put("jobStatus", "NOT_SUBMITTED");
    details.put("state", "NOT_SUBMITTED");
    details.put("queryType", "UI_RUN");
    details.put("requestType", "RUN_SQL");
    details.put("queryText", "");
    details.put("description", "No job has been submitted yet.");
    details.put("datasetPaths", List.of());
    details.put("datasetVersion", "000000000000");
    details.put("attemptDetails", List.of());
    details.put("duration", 0L);
    details.put("startTime", epoch(Instant.now()));
    details.put("outputRecords", 0L);
    details.put("isComplete", false);
    details.put("isOutputLimited", false);
    details.put("isProfileUpdateComplete", true);
    details.put("resultsAvailable", false);
    details.put("failureInfo", Map.of("errors", List.of(), "message", "", "type", "UNKNOWN"));
    details.put("queryUser", "datafabric");
    details.put("wlmQueue", "datafabric");
    details.put("totalAttempts", 0);
    return details;
  }

  private JobRecord resolveJob(String jobId) {
    if (jobId == null || jobId.isBlank() || "undefined".equalsIgnoreCase(jobId)) {
      JobRecord placeholder = new JobRecord();
      placeholder.setId("undefined");
      placeholder.setSql("");
      placeholder.setStatus(JobStatus.PENDING);
      placeholder.setCreatedAt(Instant.now());
      return placeholder;
    }
    return jobService.getJob(jobId);
  }

  private long duration(JobRecord job) {
    Instant start = job.getStartedAt() != null ? job.getStartedAt() : job.getCreatedAt();
    Instant end = job.getCompletedAt() != null ? job.getCompletedAt() : Instant.now();
    if (start == null) {
      return 0L;
    }
    return Math.max(0L, end.toEpochMilli() - start.toEpochMilli());
  }

  private boolean isComplete(JobRecord job) {
    return job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.FAILED;
  }

  private long epoch(Instant instant) {
    return instant == null ? 0L : instant.toEpochMilli();
  }

  private String shortDescription(String sql) {
    if (sql == null || sql.isBlank()) {
      return "SQL Query";
    }
    return sql.length() > 120 ? sql.substring(0, 120) + "..." : sql;
  }

  private String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
