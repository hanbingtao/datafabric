package com.datafabric.service;

import com.datafabric.dto.CreateFromSqlRequest;
import com.datafabric.dto.DatasetSummaryResponse;
import com.datafabric.dto.JobDataResponse;
import com.datafabric.model.JobRecord;
import com.datafabric.model.JobStatus;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;

@Service
public class DatasetCompatibilityService {
  private static final Pattern FROM_PATTERN =
      Pattern.compile("(?i)\\bfrom\\s+(?:\"?([A-Za-z0-9_]+)\"?\\.)?\"?([A-Za-z0-9_]+)\"?");

  private final MetadataService metadataService;
  private final JobService jobService;
  private final ConcurrentMap<String, PreviewState> previewStates = new ConcurrentHashMap<>();

  public DatasetCompatibilityService(MetadataService metadataService, JobService jobService) {
    this.metadataService = metadataService;
    this.jobService = jobService;
  }

  public Map<String, Object> createUntitledSql(
      String newVersion,
      String sessionId,
      boolean temporary,
      boolean triggerJob,
      CreateFromSqlRequest request) {
    String path = temporary ? "tmp.UNTITLED" : "tmp.tmp.UNTITLED";
    String displayPath = temporary ? "tmp / UNTITLED" : "tmp / tmp / UNTITLED";
    String version = newVersion == null || newVersion.isBlank() ? "000" + System.currentTimeMillis() : newVersion;
    String sql = request == null || request.getSql() == null ? "" : request.getSql();
    List<String> context = request == null || request.getContext() == null ? List.of() : request.getContext();

    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("id", "tmp-" + version);
    entity.put("entityId", "tmp-" + version);
    entity.put("fullPath", List.of("tmp", temporary ? "UNTITLED" : "tmp", "UNTITLED"));
    entity.put("displayFullPath", displayPath);
    entity.put("datasetType", "VIRTUAL_DATASET");
    entity.put("datasetVersion", version);
    entity.put("canReapply", false);
    entity.put("isNewQuery", true);
    entity.put("sql", sql);
    entity.put("context", context);
    entity.put("jobId", null);
    entity.put("approximate", false);
    entity.put("lastTransform", null);
    entity.put("tipVersion", version);
    entity.put("createdAt", Instant.now().toEpochMilli());
    entity.put("apiLinks", Map.of("self", "/dataset/" + path + "/version/" + version));
    entity.put(
        "links",
        Map.of(
            "self", "/new_query",
            "edit", "/new_query?mode=edit",
            "jobs", "/jobs",
            "query", "/new_query"));
    if (sessionId != null && !sessionId.isBlank()) {
      entity.put("sessionId", sessionId);
    }
    if (triggerJob) {
      String normalizedSql = sql == null || sql.isBlank() ? "select * from sales_fact" : sql;
      String jobId = jobService.submitSql(normalizedSql);
      previewStates.put(
          version,
          new PreviewState(
              version,
              path,
              displayPath,
              normalizedSql,
              context == null ? List.of() : List.copyOf(context),
              jobId));
      entity.put("datasetPath", List.of("tmp", "UNTITLED"));
      entity.put("jobId", Map.of("id", jobId));
      entity.put("paginationUrl", "/job/" + jobId + "/data");
      entity.put("history", List.of());
      entity.put("recordCount", null);
    }
    return entity;
  }

  public Map<String, Object> getDatasetSummary(String path) throws SQLException {
    String tableName = normalizeTableName(path);
    DatasetSummaryResponse summary = metadataService.getDatasetSummary(tableName);
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("fullPath", List.of("Samples", tableName));
    response.put("jobCount", 0);
    response.put("descendants", 0);
    response.put("datasetType", "PHYSICAL_DATASET");
    response.put("datasetVersion", "000000000000");
    response.put("tags", List.of());
    response.put("references", Map.of());
    response.put("entityId", "22222222-2222-2222-2222-222222222222");
    response.put("hasReflection", false);
    response.put("createdAt", Instant.now().minusSeconds(3600).toEpochMilli());
    response.put("lastModified", Instant.now().toEpochMilli());
    response.put("schemaOutdated", false);
    response.put("rootContainerType", "SOURCE");
    response.put(
        "fields",
        summary.getColumns().stream()
            .map(
                column ->
                    Map.of(
                        "name", column.name(),
                        "type", column.type(),
                        "isPartitioned", false,
                        "isSorted", false))
            .toList());
    response.put(
        "links",
        Map.of(
            "self", "/source/Samples/dataset/" + tableName,
            "query", "/new_query?context=Samples"));
    return response;
  }

  public Map<String, Object> getDatasetPreview(String path, String version, String sessionId)
      throws SQLException, IOException {
    PreviewState state = previewStates.get(version);
    String effectivePath = state == null ? path : state.datasetPath();
    String effectiveDisplayPath = state == null ? "tmp / UNTITLED" : state.displayPath();
    String effectiveSql = state == null ? "SELECT * FROM " + normalizeTableName(path) : state.sql();
    List<String> effectiveContext = state == null ? List.of("Samples") : state.context();
    String tableName = resolvePreviewTableName(effectivePath, effectiveSql);
    DatasetSummaryResponse summary = metadataService.getDatasetSummary(tableName);
    List<Map<String, Object>> columns = toColumns(summary);
    Map<String, Object> jobRef = null;

    if (state != null && state.jobId() != null && !state.jobId().isBlank()) {
      JobRecord job = jobService.getJob(state.jobId());
      jobRef = Map.of("id", state.jobId());
      if (job.getStatus() == JobStatus.COMPLETED) {
        JobDataResponse response = jobService.getJobData(state.jobId(), 0, 1);
        if (response.getColumns() != null && !response.getColumns().isEmpty()) {
          columns = toColumns(response.getColumns());
        }
      }
    }

    Map<String, Object> dataset = new LinkedHashMap<>();
    dataset.put("id", version);
    dataset.put("entityId", "tmp-" + version);
    dataset.put("datasetVersion", version);
    dataset.put("fullPath", List.of("tmp", "UNTITLED"));
    dataset.put("displayFullPath", effectiveDisplayPath);
    dataset.put("datasetType", "VIRTUAL_DATASET");
    dataset.put("canReapply", false);
    dataset.put("isNewQuery", true);
    dataset.put("sql", effectiveSql);
    dataset.put("context", effectiveContext == null || effectiveContext.isEmpty() ? List.of("Samples") : effectiveContext);
    dataset.put("approximate", false);
    dataset.put("tipVersion", version);
    dataset.put("apiLinks", Map.of("self", "/dataset/tmp.UNTITLED/version/" + version));
    dataset.put(
        "links",
        Map.of(
            "self", "/new_query",
            "edit", "/new_query?mode=edit",
            "jobs", "/jobs",
            "query", "/new_query"));
    if (sessionId != null && !sessionId.isBlank()) {
      dataset.put("sessionId", sessionId);
    }
    if (jobRef != null) {
      dataset.put("jobId", jobRef);
      dataset.put("paginationUrl", "/job/" + state.jobId() + "/data");
    }

    Map<String, Object> response = new LinkedHashMap<>();
    response.put("dataset", dataset);
    response.put("history", Map.of("items", List.of(), "version", version));
    response.put("columns", columns);
    return response;
  }

  private String normalizeTableName(String path) {
    if (path == null || path.isBlank()) {
      return "SALES_FACT";
    }
    String normalized = path.replace("\"", "").replace('/', '.');
    String[] parts = normalized.split("\\.");
    return parts[parts.length - 1].toUpperCase();
  }

  private String resolvePreviewTableName(String path, String sql) {
    Matcher matcher = FROM_PATTERN.matcher(sql == null ? "" : sql);
    if (matcher.find()) {
      return matcher.group(2).toUpperCase();
    }
    return normalizeTableName(path);
  }

  private List<Map<String, Object>> toColumns(DatasetSummaryResponse summary) {
    List<Map<String, Object>> columns = new ArrayList<>();
    for (DatasetSummaryResponse.ColumnInfo column : summary.getColumns()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("name", column.name());
      entry.put("type", column.type());
      entry.put("index", 0);
      columns.add(entry);
    }
    return columns;
  }

  private List<Map<String, Object>> toColumns(List<String> columnNames) {
    List<Map<String, Object>> columns = new ArrayList<>();
    for (int index = 0; index < columnNames.size(); index++) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("name", columnNames.get(index));
      entry.put("type", "ANY");
      entry.put("index", index);
      columns.add(entry);
    }
    return columns;
  }

  private record PreviewState(
      String version,
      String datasetPath,
      String displayPath,
      String sql,
      List<String> context,
      String jobId) {}
}
