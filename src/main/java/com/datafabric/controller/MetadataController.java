package com.datafabric.controller;

import com.datafabric.dto.DatasetSummaryResponse;
import com.datafabric.service.MetadataService;
import java.sql.SQLException;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/metadata")
public class MetadataController {
  private final MetadataService metadataService;

  public MetadataController(MetadataService metadataService) {
    this.metadataService = metadataService;
  }

  @GetMapping("/datasets")
  public List<String> listDatasets() throws SQLException {
    return metadataService.listDatasets();
  }

  @GetMapping("/datasets/{tableName}")
  public DatasetSummaryResponse getDatasetSummary(@PathVariable String tableName)
      throws SQLException {
    return metadataService.getDatasetSummary(tableName);
  }
}
