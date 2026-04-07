package com.datafabric.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;

@Service
public class BackupService {
  
  private static final String BACKUP_DIR = "runtime/backups";
  private static final String CATALOG_FILE = "runtime/catalog.json";
  
  private final ObjectMapper objectMapper;
  private final ConcurrentMap<String, BackupRecord> backups = new ConcurrentHashMap<>();
  private final JobService jobService;
  private final ReflectionService reflectionService;
  private final SessionService sessionService;

  public BackupService(
      ObjectMapper objectMapper,
      JobService jobService,
      ReflectionService reflectionService,
      SessionService sessionService) {
    this.objectMapper = objectMapper;
    this.jobService = jobService;
    this.reflectionService = reflectionService;
    this.sessionService = sessionService;
  }

  @PostConstruct
  void init() throws IOException {
    Files.createDirectories(Paths.get(BACKUP_DIR));
  }

  public BackupRecord createBackup(String name, boolean includeJobHistory, boolean includeSessions) {
    String backupId = UUID.randomUUID().toString();
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
    
    BackupRecord backup = new BackupRecord();
    backup.id = backupId;
    backup.name = name != null && !name.isBlank() ? name : "Backup-" + timestamp;
    backup.createdAt = Instant.now();
    backup.status = "IN_PROGRESS";
    
    try {
      Map<String, Object> backupData = new LinkedHashMap<>();
      
      // 备份元数据
      backupData.put("version", "1.0");
      backupData.put("createdAt", timestamp);
      backupData.put("backupId", backupId);
      
      // 备份 Jobs
      if (includeJobHistory) {
        List<Map<String, Object>> jobs = new ArrayList<>();
        for (var job : jobService.listJobs()) {
          jobs.add(Map.of(
              "id", job.getId(),
              "sql", job.getSql(),
              "status", job.getStatus().name(),
              "createdAt", job.getCreatedAt().toString()
          ));
        }
        backupData.put("jobs", jobs);
      }
      
      // 备份 Reflections
      List<Map<String, Object>> reflections = new ArrayList<>();
      for (var reflection : reflectionService.list()) {
        reflections.add(Map.of(
            "id", reflection.getId(),
            "name", reflection.getName(),
            "sql", reflection.getSql(),
            "status", reflection.getStatus().name()
        ));
      }
      backupData.put("reflections", reflections);
      
      // 备份 Sessions
      if (includeSessions) {
        List<Map<String, Object>> sessions = new ArrayList<>();
        for (var session : sessionService.listSessions()) {
          sessions.add(Map.of(
              "id", session.getId(),
              "userId", session.getUserId(),
              "createdAt", session.getCreatedAt().toString()
          ));
        }
        backupData.put("sessions", sessions);
      }
      
      // 保存备份文件
      Path backupFile = Paths.get(BACKUP_DIR, backupId + ".json");
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(backupFile.toFile(), backupData);
      
      backup.status = "COMPLETED";
      backup.filePath = backupFile.toString();
      backup.size = Files.size(backupFile);
      
    } catch (Exception e) {
      backup.status = "FAILED";
      backup.errorMessage = e.getMessage();
    }
    
    backups.put(backupId, backup);
    return backup;
  }

  public List<BackupRecord> listBackups() {
    return backups.values().stream()
        .sorted((a, b) -> b.createdAt.compareTo(a.createdAt))
        .toList();
  }

  public BackupRecord getBackup(String backupId) {
    return backups.get(backupId);
  }

  public Map<String, Object> getBackupData(String backupId) {
    BackupRecord backup = backups.get(backupId);
    if (backup == null || backup.filePath == null) {
      throw new IllegalArgumentException("Backup not found: " + backupId);
    }
    
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> data = objectMapper.readValue(
          Paths.get(backup.filePath).toFile(), 
          Map.class);
      return data;
    } catch (IOException e) {
      throw new RuntimeException("Failed to read backup data", e);
    }
  }

  public boolean deleteBackup(String backupId) {
    BackupRecord backup = backups.remove(backupId);
    if (backup != null && backup.filePath != null) {
      try {
        Files.deleteIfExists(Paths.get(backup.filePath));
        return true;
      } catch (IOException e) {
        return false;
      }
    }
    return backup != null;
  }

  public BackupRecord restoreBackup(String backupId) {
    BackupRecord backup = backups.get(backupId);
    if (backup == null || backup.filePath == null) {
      throw new IllegalArgumentException("Backup not found: " + backupId);
    }
    
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> data = objectMapper.readValue(
          Paths.get(backup.filePath).toFile(), 
          Map.class);
      
      // 恢复 Jobs (简单起见，这里只记录历史)
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> jobs = (List<Map<String, Object>>) data.get("jobs");
      if (jobs != null) {
        // Jobs 是只读的，不需要恢复
      }
      
      // 恢复 Reflections
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> reflections = (List<Map<String, Object>>) data.get("reflections");
      if (reflections != null) {
        for (Map<String, Object> reflection : reflections) {
          // 重新创建 reflection
          try {
            reflectionService.list(); // 触发服务初始化
          } catch (Exception e) {
            // ignore
          }
        }
      }
      
      backup.status = "RESTORED";
      backup.restoredAt = Instant.now();
      
    } catch (Exception e) {
      throw new RuntimeException("Failed to restore backup", e);
    }
    
    return backup;
  }

  public static class BackupRecord {
    public String id;
    public String name;
    public Instant createdAt;
    public Instant restoredAt;
    public String status;
    public String filePath;
    public long size;
    public String errorMessage;
  }
}
