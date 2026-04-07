package com.datafabric.controller;

import com.datafabric.service.MonitoringService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/api/v3/monitoring", "/apiv2/monitoring"})
public class MonitoringController {
  private final MonitoringService monitoringService;

  public MonitoringController(MonitoringService monitoringService) {
    this.monitoringService = monitoringService;
  }

  @GetMapping("/health")
  public Map<String, Object> getHealth() {
    return monitoringService.getSystemHealth();
  }

  @GetMapping("/metrics")
  public Map<String, Object> getMetrics() {
    return monitoringService.getMetrics();
  }

  @GetMapping("/metrics/detailed")
  public Map<String, Object> getDetailedMetrics() {
    return monitoringService.getDetailedMetrics();
  }
}
