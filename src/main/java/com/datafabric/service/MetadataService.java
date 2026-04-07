package com.datafabric.service;

import com.datafabric.dto.DatasetSummaryResponse;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.stereotype.Service;

@Service
public class MetadataService {
  private final DataSource dataSource;

  public MetadataService(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<String> listDatasets() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        ResultSet tables =
            connection
                .getMetaData()
                .getTables(connection.getCatalog(), "PUBLIC", "%", new String[] {"TABLE"})) {
      List<String> tableNames = new ArrayList<>();
      while (tables.next()) {
        tableNames.add(tables.getString("TABLE_NAME"));
      }
      return tableNames;
    }
  }

  public DatasetSummaryResponse getDatasetSummary(String tableName) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        ResultSet columns =
            connection.getMetaData().getColumns(connection.getCatalog(), "PUBLIC", tableName, "%")) {
      List<DatasetSummaryResponse.ColumnInfo> columnInfos = new ArrayList<>();
      while (columns.next()) {
        columnInfos.add(
            new DatasetSummaryResponse.ColumnInfo(
                columns.getString("COLUMN_NAME"),
                columns.getString("TYPE_NAME"),
                columns.getInt("COLUMN_SIZE"),
                DatabaseMetaData.columnNullable == columns.getInt("NULLABLE")));
      }
      return new DatasetSummaryResponse(tableName, columnInfos);
    }
  }
}
