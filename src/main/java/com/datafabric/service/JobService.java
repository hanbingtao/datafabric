package com.datafabric.service;

import com.datafabric.dto.JobDataResponse;
import com.datafabric.model.JobRecord;
import com.datafabric.model.JobStatus;
import com.datafabric.storage.LocalResultStore;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class JobService {
  private final TaskExecutor taskExecutor;
  private final QueryExecutionService queryExecutionService;
  private final LocalResultStore localResultStore;
  private final ConcurrentMap<String, JobRecord> jobs = new ConcurrentHashMap<>();

  public JobService(
      TaskExecutor taskExecutor,
      QueryExecutionService queryExecutionService,
      LocalResultStore localResultStore) {
    this.taskExecutor = taskExecutor;
    this.queryExecutionService = queryExecutionService;
    this.localResultStore = localResultStore;
  }

  @PostConstruct
  void init() {
    try {
      queryExecutionService.execute("CREATE SCHEMA IF NOT EXISTS Samples;");
      queryExecutionService.execute(
          "CREATE TABLE IF NOT EXISTS SALES_FACT (id INT PRIMARY KEY, customer VARCHAR(64), region VARCHAR(32), amount DECIMAL(10,2));");
      queryExecutionService.execute(
          "MERGE INTO SALES_FACT KEY(id) VALUES (1,'alice','north',120.50),(2,'bob','south',88.00),(3,'carol','east',460.10);");
    } catch (SQLException ex) {
      throw new IllegalStateException("Failed to initialize sample dataset", ex);
    }
  }

  public String submitSql(String sql) {
    String jobId = UUID.randomUUID().toString();
    JobRecord job = new JobRecord();
    job.setId(jobId);
    job.setSql(sql);
    job.setStatus(JobStatus.PENDING);
    job.setCreatedAt(Instant.now());
    jobs.put(jobId, job);
    taskExecutor.execute(() -> executeJob(job));
    return jobId;
  }

  public JobRecord getJob(String jobId) {
    JobRecord job = jobs.get(jobId);
    if (job == null) {
      throw new NoSuchElementException("Job not found: " + jobId);
    }
    return job;
  }

  public List<JobRecord> listJobs() {
    return jobs.values().stream().sorted((left, right) -> right.getCreatedAt().compareTo(left.getCreatedAt())).toList();
  }

  public JobDataResponse getJobData(String jobId, int offset, int limit) throws IOException {
    // 分页参数先兜住，避免负数下标把接口直接打成 500。
    validatePagination(offset, limit);
    JobRecord job = getJob(jobId);
    if (job.getStatus() != JobStatus.COMPLETED) {
      throw new IllegalStateException("Job is not completed. Current status: " + job.getStatus());
    }
    return localResultStore.readJobResult(job.getResultPath(), offset, limit);
  }

  public Map<String, Object> getJobSummary(String jobId, int maxSqlLength) {
    JobRecord job = getJob(jobId);
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("id", job.getId());
    summary.put("jobId", Map.of("id", job.getId()));
    summary.put("datasetVersion", "000000000000");
    summary.put("state", job.getStatus().name());
    summary.put("jobStatus", job.getStatus().name());
    summary.put("queryType", "UI_RUN");
    summary.put("requestType", "RUN_SQL");
    summary.put("description", truncatedSql(job.getSql(), maxSqlLength));
    summary.put("sql", truncatedSql(job.getSql(), maxSqlLength));
    summary.put("datasetPathList", List.of(List.of("Samples", "SALES_FACT")));
    summary.put("datasetPaths", List.of("Samples.SALES_FACT"));
    summary.put("startTime", epoch(job.getCreatedAt()));
    summary.put("endTime", epoch(job.getCompletedAt()));
    summary.put("outputRecords", job.getRowCount() == null ? 0L : job.getRowCount());
    summary.put("rowsScanned", 0L);
    summary.put("accelerated", false);
    summary.put("spilled", false);
    summary.put("queueName", "datafabric");
    return summary;
  }

  public Map<String, Object> getJobDetails(String jobId) {
    JobRecord job = getJob(jobId);
    Map<String, Object> details = new LinkedHashMap<>(getJobSummary(jobId, 200));
    details.put("attemptDetails", List.of(Map.of("reason", job.getStatus().name(), "result", job.getStatus().name())));
    details.put("duration", Math.max(0L, epoch(job.getCompletedAt()) - epoch(job.getStartedAt())));
    details.put("resultsAvailable", job.getStatus() == JobStatus.COMPLETED);
    details.put("paginationUrl", "/job/" + jobId + "/data");
    details.put("failureInfo", job.getStatus() == JobStatus.FAILED ? nullToEmpty(job.getErrorMessage()) : "");
    return details;
  }

  public Map<String, Object> cancelJob(String jobId) {
    JobRecord job = getJob(jobId);
    if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.FAILED) {
      return Map.of("type", "WARN", "message", "Job already finished");
    }
    job.setStatus(JobStatus.FAILED);
    job.setErrorMessage("Cancelled by user");
    job.setCompletedAt(Instant.now());
    return Map.of("type", "OK", "message", "Job cancellation requested");
  }

  private void executeJob(JobRecord job) {
    job.setStatus(JobStatus.RUNNING);
    job.setStartedAt(Instant.now());
    try {
      // 查询结果落盘后，job/data 接口再按 offset/limit 做分页读取。
      QueryExecutionService.QueryResult result = queryExecutionService.execute(job.getSql());
      Path saved = localResultStore.saveJobResult(job.getId(), result.columns(), result.rows());
      job.setRowCount((long) result.rows().size());
      job.setResultPath(saved.toString());
      job.setStatus(JobStatus.COMPLETED);
    } catch (SQLException | IOException ex) {
      job.setStatus(JobStatus.FAILED);
      job.setErrorMessage(ex.getMessage());
    } finally {
      job.setCompletedAt(Instant.now());
    }
  }

  private long epoch(Instant instant) {
    return instant == null ? 0L : instant.toEpochMilli();
  }

  private String truncatedSql(String sql, int maxSqlLength) {
    String value = sql == null ? "" : sql;
    if (maxSqlLength <= 0 || value.length() <= maxSqlLength) {
      return value;
    }
    return value.substring(0, maxSqlLength) + "...";
  }

  private String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private void validatePagination(int offset, int limit) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be greater than or equal to 0");
    }
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be greater than 0");
    }
  }
}
