package com.datafabric.controller;

import com.datafabric.service.BackupService;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/api/v3/backups", "/apiv2/backups"})
public class BackupController {
  private final BackupService backupService;

  public BackupController(BackupService backupService) {
    this.backupService = backupService;
  }

  @PostMapping
  public BackupService.BackupRecord createBackup(
      @RequestParam(required = false) String name,
      @RequestParam(defaultValue = "false") boolean includeJobHistory,
      @RequestParam(defaultValue = "false") boolean includeSessions) {
    return backupService.createBackup(name, includeJobHistory, includeSessions);
  }

  @GetMapping
  public List<BackupService.BackupRecord> listBackups() {
    return backupService.listBackups();
  }

  @GetMapping("/{backupId}")
  public BackupService.BackupRecord getBackup(@PathVariable String backupId) {
    return backupService.getBackup(backupId);
  }

  @GetMapping("/{backupId}/data")
  public Map<String, Object> getBackupData(@PathVariable String backupId) {
    return backupService.getBackupData(backupId);
  }

  @DeleteMapping("/{backupId}")
  public Map<String, Object> deleteBackup(@PathVariable String backupId) {
    boolean deleted = backupService.deleteBackup(backupId);
    return Map.of("deleted", deleted, "backupId", backupId);
  }

  @PostMapping("/{backupId}/restore")
  public BackupService.BackupRecord restoreBackup(@PathVariable String backupId) {
    return backupService.restoreBackup(backupId);
  }
}
