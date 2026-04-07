package com.datafabric.controller;

import com.datafabric.dto.ReflectionRequest;
import com.datafabric.model.ReflectionRecord;
import com.datafabric.service.ReflectionService;
import jakarta.validation.Valid;
import java.util.List;
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
@RequestMapping("/api/v1/reflections")
public class ReflectionController {
  private final ReflectionService reflectionService;

  public ReflectionController(ReflectionService reflectionService) {
    this.reflectionService = reflectionService;
  }

  @PostMapping
  public ReflectionRecord create(@Valid @RequestBody ReflectionRequest request) {
    return reflectionService.create(request);
  }

  @GetMapping
  public List<ReflectionRecord> list() {
    return reflectionService.list();
  }

  @GetMapping("/{reflectionId}")
  public ReflectionRecord get(@PathVariable String reflectionId) {
    return reflectionService.get(reflectionId);
  }

  @PutMapping("/{reflectionId}")
  public ReflectionRecord update(
      @PathVariable String reflectionId, @RequestBody ReflectionRequest request) {
    return reflectionService.update(reflectionId, request);
  }

  @DeleteMapping("/{reflectionId}")
  public Map<String, Object> delete(@PathVariable String reflectionId) {
    reflectionService.delete(reflectionId);
    return Map.of("deleted", true, "reflectionId", reflectionId);
  }

  @PostMapping("/{reflectionId}/refresh")
  public ReflectionRecord refresh(@PathVariable String reflectionId) {
    return reflectionService.refresh(reflectionId);
  }

  @PostMapping("/{reflectionId}/enable")
  public ReflectionRecord enable(@PathVariable String reflectionId) {
    return reflectionService.enable(reflectionId);
  }

  @PostMapping("/{reflectionId}/disable")
  public ReflectionRecord disable(@PathVariable String reflectionId) {
    return reflectionService.disable(reflectionId);
  }

  @GetMapping("/{reflectionId}/data")
  public Map<String, Object> getReflectionData(
      @PathVariable String reflectionId,
      @RequestParam(defaultValue = "0") int offset,
      @RequestParam(defaultValue = "100") int limit) {
    return reflectionService.getReflectionData(reflectionId, offset, limit);
  }
}
