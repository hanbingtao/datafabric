package com.datafabric.service;

import com.datafabric.config.DatafabricProperties;
import com.datafabric.dto.DatasetSummaryResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;

@Service
public class ClickHouseSourceService {
  private static final Pattern QUALIFIED_SOURCE_TABLE_PATTERN =
      Pattern.compile(
          "(?i)(?:from|join)\\s+\"?([A-Za-z0-9_]+)\"?\\.\"?([A-Za-z0-9_]+)\"?\\.\"?([A-Za-z0-9_]+)\"?");

  private final DatafabricProperties properties;
  private final ObjectMapper objectMapper;

  public ClickHouseSourceService(DatafabricProperties properties, ObjectMapper objectMapper) {
    this.properties = properties;
    this.objectMapper = objectMapper;
  }

  public boolean isClickHouseSource(String sourceName) {
    return loadSource(sourceName)
        .map(source -> "CLICKHOUSE".equalsIgnoreCase(source.type()))
        .orElse(false);
  }

  public Optional<String> getConfiguredDatabase(String sourceName) {
    return loadSource(sourceName)
        .map(SourceConfig::rootPath)
        .filter(value -> value != null && !value.isBlank());
  }

  public List<String> listDatabases(String sourceName) throws SQLException {
    SourceConfig source = requireSource(sourceName);
    if (source.rootPath() != null && !source.rootPath().isBlank()) {
      return List.of(source.rootPath());
    }

    try (Connection connection = openConnection(source);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      List<String> databases = new ArrayList<>();
      while (resultSet.next()) {
        databases.add(resultSet.getString(1));
      }
      return databases;
    }
  }

  public List<String> listTables(String sourceName, String databaseName) throws SQLException {
    SourceConfig source = requireSource(sourceName);
    String database = normalizeIdentifier(databaseName);
    if (database.isBlank()) {
      database = source.rootPath();
    }
    if (database == null || database.isBlank()) {
      throw new IllegalArgumentException("Database name is required");
    }

    try (Connection connection = openConnection(source);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SHOW TABLES FROM " + quoteIdentifier(database))) {
      List<String> tables = new ArrayList<>();
      while (resultSet.next()) {
        tables.add(resultSet.getString(1));
      }
      return tables;
    }
  }

  public DatasetSummaryResponse getDatasetSummary(
      String sourceName, String databaseName, String tableName) throws SQLException {
    SourceConfig source = requireSource(sourceName);
    String database = resolveDatabase(source, databaseName);
    String table = normalizeIdentifier(tableName);
    String sql =
        "SELECT * FROM " + quoteIdentifier(database) + "." + quoteIdentifier(table) + " LIMIT 0";

    try (Connection connection = openConnection(source);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      List<DatasetSummaryResponse.ColumnInfo> columns = new ArrayList<>();
      for (int index = 1; index <= metaData.getColumnCount(); index++) {
        columns.add(
            new DatasetSummaryResponse.ColumnInfo(
                metaData.getColumnLabel(index),
                metaData.getColumnTypeName(index),
                metaData.getPrecision(index),
                metaData.isNullable(index) != ResultSetMetaData.columnNoNulls));
      }
      return new DatasetSummaryResponse(table, columns);
    }
  }

  public QueryExecutionService.QueryResult previewTable(
      String sourceName, String databaseName, String tableName, int limit) throws SQLException {
    SourceConfig source = requireSource(sourceName);
    String database = resolveDatabase(source, databaseName);
    String table = normalizeIdentifier(tableName);
    String sql =
        "SELECT * FROM "
            + quoteIdentifier(database)
            + "."
            + quoteIdentifier(table)
            + " LIMIT "
            + Math.max(limit, 1);
    return executeQuery(source, sql);
  }

  public Optional<QueryExecutionService.QueryResult> tryExecuteQualifiedSql(String sql)
      throws SQLException {
    Matcher matcher = QUALIFIED_SOURCE_TABLE_PATTERN.matcher(sql == null ? "" : sql);
    if (!matcher.find()) {
      return Optional.empty();
    }

    String sourceName = matcher.group(1);
    Optional<SourceConfig> source = loadSource(sourceName);
    if (source.isEmpty() || !"CLICKHOUSE".equalsIgnoreCase(source.get().type())) {
      return Optional.empty();
    }

    String rewrittenSql =
        sql.replaceAll(
            "(?i)\"?" + Pattern.quote(sourceName) + "\"?\\.",
            "");
    return Optional.of(executeQuery(source.get(), rewrittenSql));
  }

  private QueryExecutionService.QueryResult executeQuery(SourceConfig source, String sql)
      throws SQLException {
    try (Connection connection = openConnection(source);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      List<String> columns = new ArrayList<>();
      for (int index = 1; index <= metaData.getColumnCount(); index++) {
        columns.add(metaData.getColumnLabel(index));
      }

      List<Map<String, Object>> rows = new ArrayList<>();
      while (resultSet.next()) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (String column : columns) {
          row.put(column, resultSet.getObject(column));
        }
        rows.add(row);
      }
      return new QueryExecutionService.QueryResult(columns, rows);
    }
  }

  private Connection openConnection(SourceConfig source) throws SQLException {
    loadDriver();
    String jdbcUrl =
        String.format(
            Locale.ROOT,
            "jdbc:clickhouse://%s:%d/?compress=0%s",
            source.hostname(),
            source.port(),
            source.tls() ? "&ssl=true" : "");
    return DriverManager.getConnection(jdbcUrl, source.username(), source.password());
  }

  private Optional<SourceConfig> loadSource(String sourceName) {
    try {
      JsonNode node =
          objectMapper.readTree(
              properties.getBaseDir().resolve("sources").resolve(sourceName.toLowerCase() + ".json").toFile());
      String type = node.path("type").asText("");
      JsonNode config = node.path("config");
      return Optional.of(
          new SourceConfig(
              node.path("name").asText(sourceName),
              type,
              firstNonBlank(config.path("hostname").asText(null), config.path("host").asText(null), "localhost"),
              config.path("port").asInt(8123),
              firstNonBlank(config.path("username").asText(null), "default"),
              firstNonBlank(config.path("password").asText(null), ""),
              config.path("tls").asBoolean(false) || config.path("useSsl").asBoolean(false),
              firstNonBlank(config.path("rootPath").asText(null), config.path("database").asText(null), "")));
    } catch (Exception ignored) {
      return Optional.empty();
    }
  }

  private SourceConfig requireSource(String sourceName) {
    return loadSource(sourceName)
        .orElseThrow(() -> new IllegalArgumentException("ClickHouse source not found: " + sourceName));
  }

  private String resolveDatabase(SourceConfig source, String databaseName) {
    String database = normalizeIdentifier(databaseName);
    if (database.isBlank()) {
      database = source.rootPath();
    }
    if (database == null || database.isBlank()) {
      throw new IllegalArgumentException("Database name is required");
    }
    return database;
  }

  private static void loadDriver() {
    try {
      Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
    } catch (ClassNotFoundException ex) {
      throw new IllegalStateException("ClickHouse JDBC driver is not available", ex);
    }
  }

  private String normalizeIdentifier(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("\"", "").replace("`", "").trim();
  }

  private String quoteIdentifier(String value) {
    return "`" + normalizeIdentifier(value).replace("`", "``") + "`";
  }

  private String firstNonBlank(String... values) {
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return "";
  }

  private record SourceConfig(
      String name,
      String type,
      String hostname,
      int port,
      String username,
      String password,
      boolean tls,
      String rootPath) {}
}
