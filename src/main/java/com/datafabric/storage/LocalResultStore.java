package com.datafabric.storage;

import com.datafabric.config.DatafabricProperties;
import com.datafabric.dto.JobDataResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class LocalResultStore {
  private static final TypeReference<ResultPayload> RESULT_TYPE = new TypeReference<>() {};

  private final DatafabricProperties properties;
  private final ObjectMapper objectMapper;

  public LocalResultStore(DatafabricProperties properties, ObjectMapper objectMapper) {
    this.properties = properties;
    this.objectMapper = objectMapper;
  }

  @PostConstruct
  void ensureDirs() throws IOException {
    Files.createDirectories(properties.getResultsDir());
    Files.createDirectories(properties.getAcceleratorDir());
  }

  public Path saveJobResult(String jobId, List<String> columns, List<Map<String, Object>> rows)
      throws IOException {
    Path resultFile = properties.getResultsDir().resolve(jobId + ".json");
    writePayload(resultFile, new ResultPayload(columns, rows));
    return resultFile;
  }

  public Path saveReflection(String reflectionId, List<String> columns, List<Map<String, Object>> rows)
      throws IOException {
    Path resultFile = properties.getAcceleratorDir().resolve(reflectionId + ".json");
    writePayload(resultFile, new ResultPayload(columns, rows));
    return resultFile;
  }

  public JobDataResponse readJobResult(String path, int offset, int limit) throws IOException {
    ResultPayload payload = readPayload(Path.of(path));
    int start = Math.min(offset, payload.rows().size());
    int end = Math.min(start + limit, payload.rows().size());
    JobDataResponse response = new JobDataResponse();
    response.setColumns(payload.columns());
    response.setRows(payload.rows().subList(start, end));
    response.setOffset(offset);
    response.setLimit(limit);
    response.setReturned(end - start);
    response.setTotal(payload.rows().size());
    return response;
  }

  private void writePayload(Path file, ResultPayload payload) throws IOException {
    Files.createDirectories(file.getParent());
    try (OutputStream outputStream = Files.newOutputStream(file)) {
      objectMapper.writeValue(outputStream, payload);
    }
  }

  private ResultPayload readPayload(Path file) throws IOException {
    try (InputStream inputStream = Files.newInputStream(file)) {
      ResultPayload payload = objectMapper.readValue(inputStream, RESULT_TYPE);
      return new ResultPayload(
          payload.columns() == null ? List.of() : payload.columns(),
          payload.rows() == null ? List.of() : payload.rows().stream()
              .map(LinkedHashMap::new)
              .map(row -> (Map<String, Object>) row)
              .toList());
    }
  }

  private record ResultPayload(List<String> columns, List<Map<String, Object>> rows) {}
}
