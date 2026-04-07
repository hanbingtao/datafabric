package com.datafabric.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.springframework.stereotype.Service;

@Service
public class QueryExecutionService {
  private static final Pattern SAMPLES_QUALIFIED_TABLE =
      Pattern.compile("(?i)(?:\"?Samples\"?\\.)\"?([A-Za-z0-9_]+)\"?");

  private final DataSource dataSource;

  public QueryExecutionService(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public QueryResult execute(String sql) throws SQLException {
    String normalizedSql = normalizeSql(sql);
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(normalizedSql);
      if (!hasResultSet) {
        return new QueryResult(List.of("updatedRows"), List.of(Map.of("updatedRows", statement.getUpdateCount())));
      }
      try (ResultSet resultSet = statement.getResultSet()) {
        return readResultSet(resultSet);
      }
    }
  }

  private QueryResult readResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    List<String> columns = new ArrayList<>();
    for (int index = 1; index <= metadata.getColumnCount(); index++) {
      columns.add(metadata.getColumnLabel(index));
    }

    List<Map<String, Object>> rows = new ArrayList<>();
    while (resultSet.next()) {
      Map<String, Object> row = new LinkedHashMap<>();
      for (String column : columns) {
        row.put(column, resultSet.getObject(column));
      }
      rows.add(row);
    }
    return new QueryResult(columns, rows);
  }

  private String normalizeSql(String sql) {
    if (sql == null || sql.isBlank()) {
      return "";
    }
    return SAMPLES_QUALIFIED_TABLE.matcher(sql).replaceAll("$1");
  }

  public record QueryResult(List<String> columns, List<Map<String, Object>> rows) {}
}
