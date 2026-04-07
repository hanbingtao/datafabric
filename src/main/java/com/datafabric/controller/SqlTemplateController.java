package com.datafabric.controller;

import com.datafabric.service.SqlTemplateService;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/api/v3/sql/templates", "/apiv2/sql/templates"})
public class SqlTemplateController {
  private final SqlTemplateService sqlTemplateService;

  public SqlTemplateController(SqlTemplateService sqlTemplateService) {
    this.sqlTemplateService = sqlTemplateService;
  }

  @GetMapping
  public List<SqlTemplateService.SqlTemplate> listTemplates(
      @RequestParam(required = false) String search) {
    if (search != null && !search.isBlank()) {
      return sqlTemplateService.searchTemplates(search);
    }
    return sqlTemplateService.listTemplates();
  }

  @GetMapping("/{templateId}")
  public SqlTemplateService.SqlTemplate getTemplate(@PathVariable String templateId) {
    SqlTemplateService.SqlTemplate template = sqlTemplateService.getTemplate(templateId);
    if (template == null) {
      throw new IllegalArgumentException("Template not found: " + templateId);
    }
    return template;
  }

  @PostMapping("/{templateId}/render")
  public Map<String, Object> renderTemplate(
      @PathVariable String templateId,
      @RequestBody(required = false) Map<String, String> parameters) {
    String renderedSql = sqlTemplateService.renderTemplate(templateId, parameters != null ? parameters : Map.of());
    if (renderedSql == null) {
      return Map.of("error", "Template not found: " + templateId);
    }
    return Map.of(
        "templateId", templateId,
        "sql", renderedSql,
        "parameters", parameters != null ? parameters : Map.of()
    );
  }

  @PostMapping
  public SqlTemplateService.SqlTemplate createTemplate(
      @RequestBody(required = false) Map<String, Object> request) {
    String name = request != null && request.containsKey("name") ? request.get("name").toString() : "";
    String description = request != null && request.containsKey("description") ? request.get("description").toString() : "";
    String sql = request != null && request.containsKey("sql") ? request.get("sql").toString() : "";
    
    @SuppressWarnings("unchecked")
    List<String> parameters = request != null && request.containsKey("parameters") 
        ? (List<String>) request.get("parameters") 
        : List.of();
    
    return sqlTemplateService.createTemplate(name, description, sql, parameters);
  }

  @DeleteMapping("/{templateId}")
  public Map<String, Object> deleteTemplate(@PathVariable String templateId) {
    boolean deleted = sqlTemplateService.deleteTemplate(templateId);
    return Map.of("deleted", deleted, "templateId", templateId);
  }
}
