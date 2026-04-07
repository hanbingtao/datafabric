package com.datafabric.service;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class FileStorageService {
  private static final String STORAGE_BASE = "runtime/files";
  
  private final ConcurrentMap<String, FileMetadata> files = new ConcurrentHashMap<>();
  private Path storageLocation;

  @PostConstruct
  void init() throws IOException {
    storageLocation = Paths.get(STORAGE_BASE).toAbsolutePath().normalize();
    Files.createDirectories(storageLocation);
  }

  public String upload(MultipartFile file, String folder) throws IOException {
    String originalFilename = file.getOriginalFilename();
    if (originalFilename == null || originalFilename.isBlank()) {
      originalFilename = "unnamed";
    }
    
    // 生成唯一文件 ID
    String fileId = UUID.randomUUID().toString();
    String extension = "";
    int dotIndex = originalFilename.lastIndexOf('.');
    if (dotIndex > 0) {
      extension = originalFilename.substring(dotIndex);
    }
    
    // 创建文件夹
    Path folderPath = storageLocation.resolve(folder);
    Files.createDirectories(folderPath);
    
    // 保存文件
    String storedFilename = fileId + extension;
    Path targetPath = folderPath.resolve(storedFilename);
    try (InputStream inputStream = file.getInputStream()) {
      Files.copy(inputStream, targetPath, StandardCopyOption.REPLACE_EXISTING);
    }
    
    // 记录元数据
    FileMetadata metadata = new FileMetadata();
    metadata.fileId = fileId;
    metadata.originalName = originalFilename;
    metadata.storedFilename = storedFilename;
    metadata.folder = folder;
    metadata.contentType = file.getContentType();
    metadata.size = file.getSize();
    metadata.path = targetPath.toString();
    files.put(fileId, metadata);
    
    return fileId;
  }

  public Path getFile(String fileId) {
    FileMetadata metadata = files.get(fileId);
    if (metadata == null) {
      // 尝试直接查找
      Path filePath = storageLocation.resolve(fileId);
      if (Files.exists(filePath)) {
        return filePath;
      }
      return null;
    }
    return Paths.get(metadata.path);
  }

  public Resource loadAsResource(String fileId) throws MalformedURLException {
    Path filePath = getFile(fileId);
    if (filePath == null) {
      throw new MalformedURLException("File not found: " + fileId);
    }
    return new UrlResource(filePath.toUri());
  }

  public boolean delete(String fileId) {
    FileMetadata metadata = files.remove(fileId);
    if (metadata != null) {
      try {
        Files.deleteIfExists(Paths.get(metadata.path));
        return true;
      } catch (IOException e) {
        return false;
      }
    }
    // 尝试直接删除
    Path filePath = storageLocation.resolve(fileId);
    try {
      return Files.deleteIfExists(filePath);
    } catch (IOException e) {
      return false;
    }
  }

  public List<Map<String, Object>> listFiles(String folder) throws IOException {
    List<Map<String, Object>> result = new ArrayList<>();
    Path folderPath = storageLocation.resolve(folder);
    
    if (!Files.exists(folderPath)) {
      return result;
    }
    
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(folderPath)) {
      for (Path entry : stream) {
        if (Files.isRegularFile(entry)) {
          result.add(Map.of(
              "fileName", entry.getFileName().toString(),
              "size", Files.size(entry),
              "lastModified", Files.getLastModifiedTime(entry).toMillis()
          ));
        }
      }
    }
    return result;
  }

  public void createFolder(String folder) throws IOException {
    Path folderPath = storageLocation.resolve(folder);
    Files.createDirectories(folderPath);
  }

  public Path getStorageLocation() {
    return storageLocation;
  }

  private static class FileMetadata {
    String fileId;
    String originalName;
    String storedFilename;
    String folder;
    String contentType;
    long size;
    String path;
  }
}
