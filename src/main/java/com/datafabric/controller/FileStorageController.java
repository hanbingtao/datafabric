package com.datafabric.controller;

import com.datafabric.service.FileStorageService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping({"/api/v3/files", "/apiv2/files"})
public class FileStorageController {
  private final FileStorageService fileStorageService;

  public FileStorageController(FileStorageService fileStorageService) {
    this.fileStorageService = fileStorageService;
  }

  @PostMapping("/upload")
  public Map<String, Object> uploadFile(
      @RequestParam("file") MultipartFile file,
      @RequestParam(required = false, defaultValue = "uploads") String folder) {
    try {
      String fileId = fileStorageService.upload(file, folder);
      return Map.of(
          "fileId", fileId,
          "fileName", file.getOriginalFilename(),
          "size", file.getSize(),
          "contentType", file.getContentType()
      );
    } catch (IOException e) {
      return Map.of("error", "Upload failed: " + e.getMessage());
    }
  }

  @GetMapping("/{fileId}")
  public ResponseEntity<Resource> downloadFile(@PathVariable String fileId) {
    try {
      Path filePath = fileStorageService.getFile(fileId);
      if (filePath == null || !Files.exists(filePath)) {
        return ResponseEntity.notFound().build();
      }
      
      Resource resource = fileStorageService.loadAsResource(fileId);
      String contentType = Files.probeContentType(filePath);
      if (contentType == null) {
        contentType = "application/octet-stream";
      }
      
      return ResponseEntity.ok()
          .contentType(MediaType.parseMediaType(contentType))
          .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filePath.getFileName() + "\"")
          .body(resource);
    } catch (IOException e) {
      return ResponseEntity.internalServerError().build();
    }
  }

  @GetMapping("/{fileId}/info")
  public Map<String, Object> getFileInfo(@PathVariable String fileId) {
    try {
      Path filePath = fileStorageService.getFile(fileId);
      if (filePath == null || !Files.exists(filePath)) {
        return Map.of("error", "File not found");
      }
      
      return Map.of(
          "fileId", fileId,
          "fileName", filePath.getFileName().toString(),
          "size", Files.size(filePath),
          "lastModified", Files.getLastModifiedTime(filePath).toMillis()
      );
    } catch (IOException e) {
      return Map.of("error", e.getMessage());
    }
  }

  @DeleteMapping("/{fileId}")
  public Map<String, Object> deleteFile(@PathVariable String fileId) {
    boolean deleted = fileStorageService.delete(fileId);
    return Map.of("deleted", deleted, "fileId", fileId);
  }

  @GetMapping("/list")
  public Map<String, Object> listFiles(
      @RequestParam(required = false, defaultValue = "uploads") String folder) {
    try {
      List<Map<String, Object>> files = fileStorageService.listFiles(folder);
      return Map.of("files", files, "folder", folder);
    } catch (IOException e) {
      return Map.of("error", e.getMessage(), "files", List.of());
    }
  }

  @PostMapping("/folder")
  public Map<String, Object> createFolder(@RequestParam String folder) {
    try {
      fileStorageService.createFolder(folder);
      return Map.of("created", true, "folder", folder);
    } catch (IOException e) {
      return Map.of("error", e.getMessage());
    }
  }
}
