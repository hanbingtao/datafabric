package com.datafabric.controller;

import com.datafabric.service.BootstrapService;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/apiv2")
public class LegacyResourceController {
  private final BootstrapService bootstrapService;

  public LegacyResourceController(BootstrapService bootstrapService) {
    this.bootstrapService = bootstrapService;
  }

  @GetMapping({"/user/{userName}", "/user/{userName}/"})
  public Map<String, Object> getUser(@PathVariable String userName) {
    return bootstrapService.getLegacyUser(userName);
  }

  @DeleteMapping({"/source/{sourceName}", "/source/{sourceName}/"})
  public void deleteSource(
      @PathVariable String sourceName, @RequestParam(required = false) String version) {
    bootstrapService.deleteSource(sourceName, version);
  }

  @PostMapping({"/source/{sourceName}/rename", "/source/{sourceName}/rename/"})
  public Map<String, Object> renameSource(
      @PathVariable String sourceName, @RequestParam String renameTo) {
    return bootstrapService.renameSource(sourceName, renameTo);
  }

  @GetMapping({
    "/source/{rootName}/folder",
    "/source/{rootName}/folder/",
    "/source/{rootName}/folder/{path:.+}",
    "/source/{rootName}/folder/{path:.+}/",
    "/space/{rootName}/folder",
    "/space/{rootName}/folder/",
    "/space/{rootName}/folder/{path:.+}",
    "/space/{rootName}/folder/{path:.+}/",
    "/home/{rootName}/folder",
    "/home/{rootName}/folder/",
    "/home/{rootName}/folder/{path:.+}",
    "/home/{rootName}/folder/{path:.+}/"
  })
  public Map<String, Object> getFolder(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestParam(defaultValue = "true") boolean includeContents) {
    return bootstrapService.getFolder(resolveRootType(request), rootName, path, includeContents);
  }

  @PostMapping({
    "/source/{rootName}/folder",
    "/source/{rootName}/folder/",
    "/source/{rootName}/folder/{path:.+}",
    "/source/{rootName}/folder/{path:.+}/",
    "/space/{rootName}/folder",
    "/space/{rootName}/folder/",
    "/space/{rootName}/folder/{path:.+}",
    "/space/{rootName}/folder/{path:.+}/",
    "/home/{rootName}/folder",
    "/home/{rootName}/folder/",
    "/home/{rootName}/folder/{path:.+}",
    "/home/{rootName}/folder/{path:.+}/"
  })
  public Map<String, Object> createFolder(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestBody(required = false) Map<String, Object> body) {
    return bootstrapService.createFolder(
        resolveRootType(request), rootName, path, body == null ? Map.of() : body);
  }

  @PutMapping({
    "/source/{rootName}/folder",
    "/source/{rootName}/folder/",
    "/source/{rootName}/folder/{path:.+}",
    "/source/{rootName}/folder/{path:.+}/",
    "/space/{rootName}/folder",
    "/space/{rootName}/folder/",
    "/space/{rootName}/folder/{path:.+}",
    "/space/{rootName}/folder/{path:.+}/",
    "/home/{rootName}/folder",
    "/home/{rootName}/folder/",
    "/home/{rootName}/folder/{path:.+}",
    "/home/{rootName}/folder/{path:.+}/"
  })
  public Map<String, Object> updateFolder(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestBody(required = false) Map<String, Object> body) {
    return bootstrapService.updateFolder(
        resolveRootType(request), rootName, path, body == null ? Map.of() : body);
  }

  @DeleteMapping({
    "/source/{rootName}/folder",
    "/source/{rootName}/folder/",
    "/source/{rootName}/folder/{path:.+}",
    "/source/{rootName}/folder/{path:.+}/",
    "/space/{rootName}/folder",
    "/space/{rootName}/folder/",
    "/space/{rootName}/folder/{path:.+}",
    "/space/{rootName}/folder/{path:.+}/",
    "/home/{rootName}/folder",
    "/home/{rootName}/folder/",
    "/home/{rootName}/folder/{path:.+}",
    "/home/{rootName}/folder/{path:.+}/"
  })
  public void deleteFolder(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestParam(required = false) String version) {
    bootstrapService.deleteFolder(resolveRootType(request), rootName, path, version);
  }

  @GetMapping({
    "/source/{rootName}/folder_format",
    "/source/{rootName}/folder_format/",
    "/source/{rootName}/folder_format/{path:.+}",
    "/source/{rootName}/folder_format/{path:.+}/",
    "/space/{rootName}/folder_format",
    "/space/{rootName}/folder_format/",
    "/space/{rootName}/folder_format/{path:.+}",
    "/space/{rootName}/folder_format/{path:.+}/",
    "/home/{rootName}/folder_format",
    "/home/{rootName}/folder_format/",
    "/home/{rootName}/folder_format/{path:.+}",
    "/home/{rootName}/folder_format/{path:.+}/"
  })
  public Map<String, Object> getFolderFormat(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path) {
    return bootstrapService.getFileFormat(resolveRootType(request), rootName, path, true);
  }

  @PutMapping({
    "/source/{rootName}/folder_format",
    "/source/{rootName}/folder_format/",
    "/source/{rootName}/folder_format/{path:.+}",
    "/source/{rootName}/folder_format/{path:.+}/",
    "/space/{rootName}/folder_format",
    "/space/{rootName}/folder_format/",
    "/space/{rootName}/folder_format/{path:.+}",
    "/space/{rootName}/folder_format/{path:.+}/",
    "/home/{rootName}/folder_format",
    "/home/{rootName}/folder_format/",
    "/home/{rootName}/folder_format/{path:.+}",
    "/home/{rootName}/folder_format/{path:.+}/"
  })
  public Map<String, Object> saveFolderFormat(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestBody(required = false) Map<String, Object> body) {
    return bootstrapService.saveFileFormat(
        resolveRootType(request), rootName, path, true, body == null ? Map.of() : body);
  }

  @DeleteMapping({
    "/source/{rootName}/folder_format",
    "/source/{rootName}/folder_format/",
    "/source/{rootName}/folder_format/{path:.+}",
    "/source/{rootName}/folder_format/{path:.+}/",
    "/space/{rootName}/folder_format",
    "/space/{rootName}/folder_format/",
    "/space/{rootName}/folder_format/{path:.+}",
    "/space/{rootName}/folder_format/{path:.+}/",
    "/home/{rootName}/folder_format",
    "/home/{rootName}/folder_format/",
    "/home/{rootName}/folder_format/{path:.+}",
    "/home/{rootName}/folder_format/{path:.+}/"
  })
  public void deleteFolderFormat(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestParam(required = false) String version) {
    bootstrapService.deleteFileFormat(resolveRootType(request), rootName, path, true, version);
  }

  @GetMapping({
    "/source/{rootName}/file_format",
    "/source/{rootName}/file_format/",
    "/source/{rootName}/file_format/{path:.+}",
    "/source/{rootName}/file_format/{path:.+}/",
    "/space/{rootName}/file_format",
    "/space/{rootName}/file_format/",
    "/space/{rootName}/file_format/{path:.+}",
    "/space/{rootName}/file_format/{path:.+}/",
    "/home/{rootName}/file_format",
    "/home/{rootName}/file_format/",
    "/home/{rootName}/file_format/{path:.+}",
    "/home/{rootName}/file_format/{path:.+}/"
  })
  public Map<String, Object> getFileFormat(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path) {
    return bootstrapService.getFileFormat(resolveRootType(request), rootName, path, false);
  }

  @PutMapping({
    "/source/{rootName}/file_format",
    "/source/{rootName}/file_format/",
    "/source/{rootName}/file_format/{path:.+}",
    "/source/{rootName}/file_format/{path:.+}/",
    "/space/{rootName}/file_format",
    "/space/{rootName}/file_format/",
    "/space/{rootName}/file_format/{path:.+}",
    "/space/{rootName}/file_format/{path:.+}/",
    "/home/{rootName}/file_format",
    "/home/{rootName}/file_format/",
    "/home/{rootName}/file_format/{path:.+}",
    "/home/{rootName}/file_format/{path:.+}/"
  })
  public Map<String, Object> saveFileFormat(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestBody(required = false) Map<String, Object> body) {
    return bootstrapService.saveFileFormat(
        resolveRootType(request), rootName, path, false, body == null ? Map.of() : body);
  }

  @DeleteMapping({
    "/source/{rootName}/file_format",
    "/source/{rootName}/file_format/",
    "/source/{rootName}/file_format/{path:.+}",
    "/source/{rootName}/file_format/{path:.+}/",
    "/space/{rootName}/file_format",
    "/space/{rootName}/file_format/",
    "/space/{rootName}/file_format/{path:.+}",
    "/space/{rootName}/file_format/{path:.+}/",
    "/home/{rootName}/file_format",
    "/home/{rootName}/file_format/",
    "/home/{rootName}/file_format/{path:.+}",
    "/home/{rootName}/file_format/{path:.+}/"
  })
  public void deleteFileFormat(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestParam(required = false) String version) {
    bootstrapService.deleteFileFormat(resolveRootType(request), rootName, path, false, version);
  }

  @PostMapping({
    "/source/{rootName}/file_preview",
    "/source/{rootName}/file_preview/",
    "/source/{rootName}/file_preview/{path:.+}",
    "/source/{rootName}/file_preview/{path:.+}/",
    "/source/{rootName}/folder_preview",
    "/source/{rootName}/folder_preview/",
    "/source/{rootName}/folder_preview/{path:.+}",
    "/source/{rootName}/folder_preview/{path:.+}/",
    "/home/{rootName}/file_preview",
    "/home/{rootName}/file_preview/",
    "/home/{rootName}/file_preview/{path:.+}",
    "/home/{rootName}/file_preview/{path:.+}/",
    "/home/{rootName}/folder_preview",
    "/home/{rootName}/folder_preview/",
    "/home/{rootName}/folder_preview/{path:.+}",
    "/home/{rootName}/folder_preview/{path:.+}/"
  })
  public Map<String, Object> previewFileFormat(
      HttpServletRequest request,
      @PathVariable String rootName,
      @PathVariable(required = false) String path,
      @RequestBody(required = false) Map<String, Object> body) {
    return bootstrapService.previewFileFormat(
        resolveRootType(request), rootName, path, body == null ? Map.of() : body);
  }

  private String resolveRootType(HttpServletRequest request) {
    String uri = request.getRequestURI();
    if (uri.startsWith("/apiv2/source/")) {
      return "source";
    }
    if (uri.startsWith("/apiv2/space/")) {
      return "space";
    }
    return "home";
  }
}
