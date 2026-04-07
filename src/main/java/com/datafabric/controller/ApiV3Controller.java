package com.datafabric.controller;

import com.datafabric.dto.ApiUserResponse;
import com.datafabric.dto.ApiV3ListResponse;
import com.datafabric.dto.CatalogItemResponse;
import com.datafabric.dto.CollaborationTagResponse;
import com.datafabric.dto.CollaborationWikiResponse;
import com.datafabric.dto.SourceTypeTemplateResponse;
import com.datafabric.dto.UserPreferenceResponse;
import com.datafabric.service.BootstrapService;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestBody;

@RestController
@RequestMapping("/api/v3")
public class ApiV3Controller {
  private final BootstrapService bootstrapService;

  public ApiV3Controller(BootstrapService bootstrapService) {
    this.bootstrapService = bootstrapService;
  }

  @GetMapping("/source/type")
  public ApiV3ListResponse<SourceTypeTemplateResponse> listSourceTypes() {
    return new ApiV3ListResponse<>(bootstrapService.listSourceTypes());
  }

  @GetMapping("/source/type/{name}")
  public SourceTypeTemplateResponse getSourceType(@PathVariable String name) {
    return bootstrapService.getSourceType(name);
  }

  @GetMapping("/user/by-name/{name}")
  public ApiUserResponse getUserByName(@PathVariable String name) {
    return bootstrapService.getUserByName(name);
  }

  @GetMapping({"/catalog", "/catalog/"})
  public ApiV3ListResponse<CatalogItemResponse> listCatalogRoot() {
    return new ApiV3ListResponse<>(bootstrapService.listCatalogRoot());
  }

  @GetMapping("/catalog/{id}")
  public Map<String, Object> getCatalogById(
      @PathVariable String id,
      @RequestParam(required = false) Integer maxChildren) {
    return bootstrapService.getCatalogById(id, maxChildren);
  }

  @GetMapping({"/catalog/by-path/{segment:.*}", "/catalog/by-path/{segment:.*}/"})
  public Map<String, Object> getCatalogByPath(
      @PathVariable("segment") List<String> path,
      @RequestParam(required = false) Integer maxChildren) {
    return bootstrapService.getCatalogByPath(path, maxChildren);
  }

  @GetMapping("/catalog/{id}/collaboration/wiki")
  public CollaborationWikiResponse getWiki(@PathVariable String id) {
    return bootstrapService.getWiki(id);
  }

  @PostMapping("/catalog/{id}/collaboration/wiki")
  public CollaborationWikiResponse saveWiki(
      @PathVariable String id, @RequestBody(required = false) CollaborationWikiResponse wiki) {
    return bootstrapService.saveWiki(id, wiki);
  }

  @GetMapping("/catalog/{id}/collaboration/tag")
  public CollaborationTagResponse getTags(@PathVariable String id) {
    return bootstrapService.getTags(id);
  }

  @PostMapping("/catalog/{id}/collaboration/tag")
  public CollaborationTagResponse saveTags(
      @PathVariable String id, @RequestBody(required = false) CollaborationTagResponse tags) {
    return bootstrapService.saveTags(id, tags);
  }

  @GetMapping({"/users/preferences/{preferenceType}", "/users/preferences/{preferenceType}/"})
  public UserPreferenceResponse getPreference(
      @PathVariable String preferenceType,
      @RequestParam(defaultValue = "false") boolean showCatalogInfo) {
    return bootstrapService.getPreference(preferenceType, showCatalogInfo);
  }

  @PutMapping({
    "/users/preferences/{preferenceType}/{entityId}",
    "/users/preferences/{preferenceType}/{entityId}/"
  })
  public UserPreferenceResponse addPreference(
      @PathVariable String preferenceType, @PathVariable String entityId) {
    return bootstrapService.addPreference(preferenceType, entityId);
  }

  @DeleteMapping({
    "/users/preferences/{preferenceType}/{entityId}",
    "/users/preferences/{preferenceType}/{entityId}/"
  })
  public UserPreferenceResponse removePreference(
      @PathVariable String preferenceType, @PathVariable String entityId) {
    return bootstrapService.removePreference(preferenceType, entityId);
  }
}
