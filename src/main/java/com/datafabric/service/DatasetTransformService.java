package com.datafabric.service;

import com.datafabric.dto.DatasetSummaryResponse;
import com.datafabric.dto.JobDataResponse;
import com.datafabric.dto.TransformRequest;
import com.datafabric.model.JobRecord;
import com.datafabric.model.JobStatus;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;

@Service
public class DatasetTransformService {
  private final MetadataService metadataService;
  private final JobService jobService;
  private final ConcurrentMap<String, TransformState> transformStates = new ConcurrentHashMap<>();

  public DatasetTransformService(
      MetadataService metadataService,
      JobService jobService) {
    this.metadataService = metadataService;
    this.jobService = jobService;
  }

  public Map<String, Object> getInitialTransform(String path, String version, String sessionId) {
    String datasetPath = normalizePath(path);
    
    // 获取数据集信息
    DatasetSummaryResponse summary;
    try {
      summary = metadataService.getDatasetSummary(datasetPath);
    } catch (Exception e) {
      // 如果获取失败，返回默认结构
      summary = createDefaultSummary(datasetPath);
    }
    
    Map<String, Object> response = new LinkedHashMap<>();
    
    // Dataset 信息
    Map<String, Object> dataset = new LinkedHashMap<>();
    dataset.put("id", datasetPath.replace(".", "_") + "_" + version);
    dataset.put("entityId", datasetPath.replace(".", "_"));
    dataset.put("datasetVersion", version);
    dataset.put("fullPath", List.of(datasetPath.split("\\.")));
    dataset.put("displayFullPath", datasetPath.replace(".", " / "));
    dataset.put("datasetType", "VIRTUAL_DATASET");
    dataset.put("sql", getDefaultSql(datasetPath));
    dataset.put("context", List.of("Samples"));
    dataset.put("isNewQuery", false);
    dataset.put("canReapply", true);
    response.put("dataset", dataset);
    
    // Schema 信息
    response.put("schemaOutdated", false);
    response.put("fields", summary.getColumns().stream()
        .map(col -> Map.of(
            "name", col.name(),
            "type", col.type(),
            "isPartitioned", false,
            "isSorted", false
        ))
        .toList());
    
    // Transform 可用操作
    response.put("transformOptions", Map.of(
        "canFilter", true,
        "canSort", true,
        "canJoin", true,
        "canGroupBy", true,
        "canDeriveColumn", true,
        "canExtract", true,
        "canReplace", true
    ));
    
    // SQL 历史
    response.put("history", Map.of("items", List.of(), "version", version));
    
    return response;
  }

  public Map<String, Object> transform(
      String path,
      String version,
      String sessionId,
      TransformRequest request) {
    
    String datasetPath = normalizePath(path);
    String stateKey = sessionId != null ? sessionId : datasetPath + "_" + version;
    
    // 获取或创建转换状态
    TransformState state = transformStates.computeIfAbsent(stateKey, 
        k -> new TransformState(datasetPath, version));
    
    // 根据转换类型处理
    String sql;
    if (request != null && request.getSql() != null && !request.getSql().isBlank()) {
      // 直接使用提供的 SQL
      sql = request.getSql();
      state.setSql(sql);
    } else if (request != null && request.getTransformType() != null) {
      sql = applyTransform(state, request);
      state.setSql(sql);
    } else {
      sql = state.getSql();
    }
    
    // 执行 SQL 获取结果
    Map<String, Object> response = new LinkedHashMap<>();
    
    // 更新数据集信息
    Map<String, Object> dataset = new LinkedHashMap<>();
    dataset.put("id", datasetPath.replace(".", "_") + "_" + version);
    dataset.put("entityId", datasetPath.replace(".", "_"));
    dataset.put("datasetVersion", version);
    dataset.put("fullPath", List.of(datasetPath.split("\\.")));
    dataset.put("displayFullPath", datasetPath.replace(".", " / "));
    dataset.put("datasetType", "VIRTUAL_DATASET");
    dataset.put("sql", sql);
    dataset.put("context", state.getContext());
    dataset.put("isNewQuery", false);
    dataset.put("canReapply", true);
    response.put("dataset", dataset);
    
    // SQL 历史
    response.put("history", Map.of(
        "items", state.getHistoryItems(),
        "version", version
    ));
    
    // 执行预览
    try {
      String previewSql = sql + " LIMIT 50";
      String jobId = jobService.submitSql(previewSql);
      state.setSql(sql);
      state.addHistoryItem(sql, version);
      
      // 获取预览数据
      JobRecord job = jobService.getJob(jobId);
      if (job.getStatus() == JobStatus.COMPLETED) {
        JobDataResponse data = jobService.getJobData(jobId, 0, 50);
        response.put("preview", Map.of(
            "columns", data.getColumns(),
            "rows", data.getRows(),
            "returned", data.getReturned()
        ));
        response.put("jobId", Map.of("id", jobId));
      }
    } catch (Exception e) {
      response.put("error", e.getMessage());
    }
    
    return response;
  }

  public Map<String, Object> preview(String path, String version, String sessionId, int limit) {
    String datasetPath = normalizePath(path);
    String stateKey = sessionId != null ? sessionId : datasetPath + "_" + version;
    TransformState state = transformStates.get(stateKey);
    
    String sql = state != null ? state.getSql() : getDefaultSql(datasetPath);
    
    Map<String, Object> response = new LinkedHashMap<>();
    
    try {
      String jobId = jobService.submitSql(sql);
      
      // 等待 Job 完成（最多 10 秒）
      JobRecord job = waitForJobCompletion(jobId, 10000);
      if (job == null || job.getStatus() != JobStatus.COMPLETED) {
        response.put("error", "Job did not complete in time");
        response.put("jobId", Map.of("id", jobId));
        return response;
      }
      
      JobDataResponse data = jobService.getJobData(jobId, 0, limit);
      
      response.put("columns", data.getColumns());
      response.put("rows", data.getRows());
      response.put("returned", data.getReturned());
      response.put("total", data.getTotal());
      response.put("jobId", Map.of("id", jobId));
    } catch (Exception e) {
      response.put("error", e.getMessage());
    }
    
    return response;
  }
  
  private JobRecord waitForJobCompletion(String jobId, long timeoutMs) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeoutMs) {
      JobRecord job = jobService.getJob(jobId);
      if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.FAILED) {
        return job;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return jobService.getJob(jobId);
  }

  public Map<String, Object> getHistogram(String path, String version, String column, int numBins) {
    String datasetPath = normalizePath(path);
    
    // 生成直方图数据
    List<Map<String, Object>> buckets = new ArrayList<>();
    
    try {
      String sql = String.format(
          "SELECT %s as val, COUNT(*) as cnt FROM %s GROUP BY %s ORDER BY cnt DESC LIMIT %d",
          column, datasetPath, column, numBins);
      String jobId = jobService.submitSql(sql);
      
      // 等待 Job 完成
      JobRecord job = waitForJobCompletion(jobId, 10000);
      if (job == null || job.getStatus() != JobStatus.COMPLETED) {
        return Map.of("histogram", buckets, "column", column, "error", "Job did not complete");
      }
      
      JobDataResponse data = jobService.getJobData(jobId, 0, numBins);
      
      if (data.getRows() != null) {
        for (Map<String, Object> row : data.getRows()) {
          // 列名可能是大写或小写
          Object val = row.get("val") != null ? row.get("val") : row.get("VAL");
          Object cnt = row.get("cnt") != null ? row.get("cnt") : row.get("CNT");
          buckets.add(Map.of(
              "value", val,
              "count", cnt
          ));
        }
      }
    } catch (Exception e) {
      // 返回空直方图
    }
    
    return Map.of("histogram", buckets, "column", column);
  }

  public Map<String, Object> getSampleValues(String path, String version, String column, int limit) {
    String datasetPath = normalizePath(path);
    
    List<Object> samples = new ArrayList<>();
    
    try {
      String sql = String.format(
          "SELECT DISTINCT %s as val FROM %s ORDER BY %s LIMIT %d",
          column, datasetPath, column, limit);
      String jobId = jobService.submitSql(sql);
      
      // 等待 Job 完成
      JobRecord job = waitForJobCompletion(jobId, 10000);
      if (job == null || job.getStatus() != JobStatus.COMPLETED) {
        return Map.of("samples", samples, "column", column, "error", "Job did not complete");
      }
      
      JobDataResponse data = jobService.getJobData(jobId, 0, limit);
      
      if (data.getRows() != null) {
        for (Map<String, Object> row : data.getRows()) {
          // 列名可能是大写或小写
          Object val = row.get("val") != null ? row.get("val") : row.get("VAL");
          samples.add(val);
        }
      }
    } catch (Exception e) {
      // 返回空样本
    }
    
    return Map.of("samples", samples, "column", column);
  }

  private String applyTransform(TransformState state, TransformRequest request) {
    String transformType = request.getTransformType();
    
    return switch (transformType.toLowerCase()) {
      case "filter" -> applyFilter(state, request);
      case "sort" -> applySort(state, request);
      case "join" -> applyJoin(state, request);
      case "groupby" -> applyGroupBy(state, request);
      case "derivecolumn" -> applyDeriveColumn(state, request);
      case "dropcolumn" -> applyDropColumn(state, request);
      case "renamecolumn" -> applyRenameColumn(state, request);
      default -> state.getSql();
    };
  }

  private String applyFilter(TransformState state, TransformRequest request) {
    String baseSql = state.getSql();
    List<Map<String, Object>> filters = request.getColumns();
    
    if (filters == null || filters.isEmpty()) {
      return baseSql;
    }
    
    StringBuilder whereClause = new StringBuilder();
    for (Map<String, Object> filter : filters) {
      if (whereClause.length() > 0) {
        whereClause.append(" AND ");
      }
      String column = (String) filter.get("column");
      String operator = (String) filter.getOrDefault("operator", "=");
      Object value = filter.get("value");
      
      whereClause.append(column).append(" ").append(operator).append(" ");
      if (value instanceof String) {
        whereClause.append("'").append(value).append("'");
      } else {
        whereClause.append(value);
      }
    }
    
    return "SELECT * FROM (" + baseSql + ") WHERE " + whereClause;
  }

  private String applySort(TransformState state, TransformRequest request) {
    String baseSql = state.getSql();
    List<Map<String, Object>> sorts = request.getColumns();
    
    if (sorts == null || sorts.isEmpty()) {
      return baseSql;
    }
    
    StringBuilder orderClause = new StringBuilder();
    for (Map<String, Object> sort : sorts) {
      if (orderClause.length() > 0) {
        orderClause.append(", ");
      }
      String column = (String) sort.get("column");
      String direction = (String) sort.getOrDefault("direction", "ASC");
      orderClause.append(column).append(" ").append(direction);
    }
    
    return "SELECT * FROM (" + baseSql + ") ORDER BY " + orderClause;
  }

  private String applyJoin(TransformState state, TransformRequest request) {
    // Join 比较复杂，需要左右数据集信息和连接条件
    // 这里返回一个简单的占位实现
    return state.getSql() + " /* join transformation pending */";
  }

  private String applyGroupBy(TransformState state, TransformRequest request) {
    String baseSql = state.getSql();
    Map<String, Object> agg = request.getAggregation();
    
    if (agg == null) {
      return baseSql;
    }
    
    List<String> groupCols = new ArrayList<>();
    List<String> selectCols = new ArrayList<>();
    
    if (request.getColumns() != null) {
      for (Map<String, Object> col : request.getColumns()) {
        String colName = (String) col.get("column");
        groupCols.add(colName);
        selectCols.add(colName);
      }
    }
    
    if (agg.containsKey("aggregations")) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> aggs = (List<Map<String, Object>>) agg.get("aggregations");
      for (Map<String, Object> a : aggs) {
        String func = (String) a.get("function");
        String col = (String) a.get("column");
        String alias = (String) a.getOrDefault("alias", func + "_" + col);
        selectCols.add(func + "(" + col + ") as " + alias);
      }
    }
    
    return "SELECT " + String.join(", ", selectCols) + " FROM (" + baseSql + ") GROUP BY " + String.join(", ", groupCols);
  }

  private String applyDeriveColumn(TransformState state, TransformRequest request) {
    String baseSql = state.getSql();
    List<Map<String, Object>> values = request.getValues();
    
    if (values == null || values.isEmpty()) {
      return baseSql;
    }
    
    StringBuilder sql = new StringBuilder("SELECT *, ");
    for (int i = 0; i < values.size(); i++) {
      Map<String, Object> val = values.get(i);
      String expr = (String) val.get("expression");
      String alias = (String) val.get("alias");
      if (alias == null) {
        alias = "derived_col_" + i;
      }
      sql.append(expr).append(" as ").append(alias);
      if (i < values.size() - 1) {
        sql.append(", ");
      }
    }
    sql.append(" FROM (").append(baseSql).append(")");
    
    return sql.toString();
  }

  private String applyDropColumn(TransformState state, TransformRequest request) {
    String baseSql = state.getSql();
    List<Map<String, Object>> cols = request.getColumns();
    
    if (cols == null || cols.isEmpty()) {
      return baseSql;
    }
    
    List<String> keepCols = new ArrayList<>();
    for (Map<String, Object> col : cols) {
      if (!Boolean.TRUE.equals(col.get("drop"))) {
        keepCols.add((String) col.get("column"));
      }
    }
    
    if (keepCols.isEmpty()) {
      return baseSql;
    }
    
    return "SELECT " + String.join(", ", keepCols) + " FROM (" + baseSql + ")";
  }

  private String applyRenameColumn(TransformState state, TransformRequest request) {
    // 列重命名需要知道原始列名和新列名
    // 这里返回原 SQL
    return state.getSql() + " /* rename column pending */";
  }

  private String normalizePath(String path) {
    if (path == null || path.isBlank()) {
      return "SALES_FACT";
    }
    // 将 URL 编码的路径或点号分隔的路径标准化
    // SAMPLES.SALES_FACT -> SALES_FACT (表名)
    String normalized = path.replace("/", ".");
    if (normalized.contains(".")) {
      String[] parts = normalized.split("\\.");
      return parts[parts.length - 1];  // 返回最后一部分作为表名
    }
    return normalized;
  }

  private String getDefaultSql(String datasetPath) {
    // datasetPath 应该是表名，如 SALES_FACT
    return "SELECT * FROM " + datasetPath;
  }

  private DatasetSummaryResponse createDefaultSummary(String datasetPath) {
    return new DatasetSummaryResponse(
        datasetPath,
        List.of(
            new DatasetSummaryResponse.ColumnInfo("ID", "INTEGER", 0, false),
            new DatasetSummaryResponse.ColumnInfo("CUSTOMER", "VARCHAR", 0, true),
            new DatasetSummaryResponse.ColumnInfo("REGION", "VARCHAR", 0, true),
            new DatasetSummaryResponse.ColumnInfo("AMOUNT", "DECIMAL", 0, false)
        )
    );
  }

  // 内部类：转换状态
  private static class TransformState {
    private final String datasetPath;
    private final String version;
    private String sql;
    private List<String> context;
    private List<TransformHistoryItem> history;

    TransformState(String datasetPath, String version) {
      this.datasetPath = datasetPath;
      this.version = version;
      // datasetPath 现在是表名（如 SALES_FACT），直接使用
      this.sql = "SELECT * FROM " + datasetPath;
      this.context = List.of("Samples");
      this.history = new ArrayList<>();
    }

    String getDatasetPath() { return datasetPath; }
    String getVersion() { return version; }
    String getSql() { return sql; }
    void setSql(String sql) { this.sql = sql; }
    List<String> getContext() { return context; }
    List<Map<String, Object>> getHistoryItems() {
      List<Map<String, Object>> result = new ArrayList<>();
      for (TransformHistoryItem h : history) {
        result.add(Map.of(
            "sql", (Object) h.sql(),
            "version", (Object) h.version(),
            "createdAt", (Object) h.timestamp()
        ));
      }
      return result;
    }
    void addHistoryItem(String sql, String version) {
      history.add(new TransformHistoryItem(sql, version, Instant.now().toEpochMilli()));
    }
  }

  private record TransformHistoryItem(String sql, String version, long timestamp) {}
}
