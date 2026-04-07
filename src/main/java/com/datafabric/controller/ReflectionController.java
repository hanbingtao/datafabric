package com.datafabric.controller;

import com.datafabric.dto.ReflectionRequest;
import com.datafabric.model.ReflectionRecord;
import com.datafabric.service.ReflectionService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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

  @PostMapping("/{reflectionId}/refresh")
  public ReflectionRecord refresh(@PathVariable String reflectionId) {
    return reflectionService.refresh(reflectionId);
  }
}
