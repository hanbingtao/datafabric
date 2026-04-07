package com.datafabric.service;

import com.datafabric.config.DatafabricProperties;
import com.datafabric.dto.ReflectionRequest;
import com.datafabric.model.ReflectionRecord;
import com.datafabric.model.ReflectionStatus;
import com.datafabric.storage.LocalResultStore;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ReflectionService {
  private final DatafabricProperties properties;
  private final QueryExecutionService queryExecutionService;
  private final LocalResultStore localResultStore;
  private final ConcurrentMap<String, ReflectionRecord> reflections = new ConcurrentHashMap<>();

  public ReflectionService(
      DatafabricProperties properties,
      QueryExecutionService queryExecutionService,
      LocalResultStore localResultStore) {
    this.properties = properties;
    this.queryExecutionService = queryExecutionService;
    this.localResultStore = localResultStore;
  }

  public ReflectionRecord create(ReflectionRequest request) {
    if (properties.getReflection().isMaxCountEnabled()
        && reflections.size() >= properties.getReflection().getMaxCount()) {
      throw new IllegalStateException(
          "Maximum number of allowed reflections exceeded (%d)"
              .formatted(properties.getReflection().getMaxCount()));
    }

    ReflectionRecord reflection = new ReflectionRecord();
    reflection.setId(UUID.randomUUID().toString());
    reflection.setName(request.getName());
    reflection.setSql(request.getSql());
    reflection.setStatus(ReflectionStatus.ACTIVE);
    reflection.setCreatedAt(Instant.now());
    reflection.setRefreshIntervalSeconds(request.getRefreshIntervalSeconds());
    reflection.setNextRefreshAt(Instant.now());
    reflections.put(reflection.getId(), reflection);
    refresh(reflection.getId());
    return reflection;
  }

  public List<ReflectionRecord> list() {
    return reflections.values().stream()
        .sorted((left, right) -> right.getCreatedAt().compareTo(left.getCreatedAt()))
        .toList();
  }

  public ReflectionRecord get(String reflectionId) {
    ReflectionRecord reflection = reflections.get(reflectionId);
    if (reflection == null) {
      throw new NoSuchElementException("Reflection not found: " + reflectionId);
    }
    return reflection;
  }

  public ReflectionRecord refresh(String reflectionId) {
    ReflectionRecord reflection = get(reflectionId);
    reflection.setStatus(ReflectionStatus.REFRESHING);
    try {
      QueryExecutionService.QueryResult result = queryExecutionService.execute(reflection.getSql());
      Path path =
          localResultStore.saveReflection(reflection.getId(), result.columns(), result.rows());
      reflection.setMaterializationPath(path.toString());
      reflection.setLastRefreshAt(Instant.now());
      reflection.setNextRefreshAt(Instant.now().plusSeconds(reflection.getRefreshIntervalSeconds()));
      reflection.setStatus(ReflectionStatus.ACTIVE);
      reflection.setErrorMessage(null);
    } catch (SQLException | IOException ex) {
      reflection.setStatus(ReflectionStatus.FAILED);
      reflection.setErrorMessage(ex.getMessage());
    }
    return reflection;
  }

  @Scheduled(fixedDelayString = "${datafabric.reflection.scheduler-delay:PT30S}")
  public void refreshDueReflections() {
    Instant now = Instant.now();
    for (ReflectionRecord reflection : reflections.values()) {
      if (reflection.getStatus() == ReflectionStatus.DISABLED) {
        continue;
      }
      if (reflection.getNextRefreshAt() == null || !reflection.getNextRefreshAt().isAfter(now)) {
        refresh(reflection.getId());
      }
    }
  }
}
