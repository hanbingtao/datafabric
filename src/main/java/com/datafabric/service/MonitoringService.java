package com.datafabric.service;

import com.datafabric.config.DatafabricProperties;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class MonitoringService {
  
  private final DataSource dataSource;
  private final DatafabricProperties properties;
  private final JobService jobService;
  private final ReflectionService reflectionService;
  private final NotificationService notificationService;
  
  private Instant startTime = Instant.now();
  private long totalRequests = 0;
  private long totalErrors = 0;
  private long maxMemoryUsed = 0;

  public MonitoringService(
      DataSource dataSource,
      DatafabricProperties properties,
      JobService jobService,
      ReflectionService reflectionService,
      NotificationService notificationService) {
    this.dataSource = dataSource;
    this.properties = properties;
    this.jobService = jobService;
    this.reflectionService = reflectionService;
    this.notificationService = notificationService;
  }

  public void incrementRequestCount() {
    totalRequests++;
  }

  public void incrementErrorCount() {
    totalErrors++;
  }

  @Scheduled(fixedDelay = 60000) // 每分钟
  void collectMetrics() {
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
    long used = heapUsage.getUsed();
    if (used > maxMemoryUsed) {
      maxMemoryUsed = used;
    }
  }

  public Map<String, Object> getSystemHealth() {
    Map<String, Object> health = new LinkedHashMap<>();
    
    // 基础状态
    health.put("status", "UP");
    health.put("timestamp", Instant.now().toString());
    
    // 正常运行时间
    long uptimeSeconds = Instant.now().getEpochSecond() - startTime.getEpochSecond();
    health.put("uptimeSeconds", uptimeSeconds);
    health.put("uptimeFormatted", formatUptime(uptimeSeconds));
    
    // 内存使用
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
    health.put("memory", Map.of(
        "heapUsed", heapUsage.getUsed(),
        "heapMax", heapUsage.getMax(),
        "heapCommitted", heapUsage.getCommitted(),
        "heapUsedPercent", (heapUsage.getUsed() * 100 / heapUsage.getMax())
    ));
    health.put("maxMemoryUsed", maxMemoryUsed);
    
    // 线程
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    health.put("threads", Map.of(
        "count", threadBean.getThreadCount(),
        "peak", threadBean.getPeakThreadCount(),
        "daemon", threadBean.getDaemonThreadCount()
    ));
    
    // 数据库连接
    try {
      var hikari = dataSource;
      health.put("database", Map.of(
          "status", "UP",
          "type", "HikariCP"
      ));
    } catch (Exception e) {
      health.put("database", Map.of(
          "status", "DOWN",
          "error", e.getMessage()
      ));
    }
    
    return health;
  }

  public Map<String, Object> getMetrics() {
    Map<String, Object> metrics = new LinkedHashMap<>();
    
    // 请求统计
    metrics.put("requests", Map.of(
        "total", totalRequests,
        "errors", totalErrors,
        "errorRate", totalRequests > 0 ? (totalErrors * 100.0 / totalRequests) : 0
    ));
    
    // Jobs 统计
    List<?> jobs = jobService.listJobs();
    long completedJobs = jobs.stream()
        .filter(j -> j instanceof com.datafabric.model.JobRecord)
        .filter(j -> ((com.datafabric.model.JobRecord) j).getStatus() == com.datafabric.model.JobStatus.COMPLETED)
        .count();
    
    metrics.put("jobs", Map.of(
        "total", jobs.size(),
        "completed", completedJobs,
        "pending", jobs.size() - completedJobs
    ));
    
    // Reflections 统计
    List<?> reflections = reflectionService.list();
    metrics.put("reflections", Map.of(
        "total", reflections.size()
    ));
    
    // WebSocket 连接
    metrics.put("websocket", Map.of(
        "activeConnections", notificationService.getActiveConnectionCount()
    ));
    
    // 内存
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
    metrics.put("memory", Map.of(
        "heapUsed", heapUsage.getUsed(),
        "heapMax", heapUsage.getMax(),
        "heapUsedPercent", (heapUsage.getUsed() * 100 / heapUsage.getMax()),
        "maxMemoryUsed", maxMemoryUsed
    ));
    
    return metrics;
  }

  public Map<String, Object> getDetailedMetrics() {
    Map<String, Object> metrics = getMetrics();
    
    // 添加更详细的信息
    metrics.put("startTime", startTime.toString());
    metrics.put("system", Map.of(
        "availableProcessors", Runtime.getRuntime().availableProcessors(),
        "osName", System.getProperty("os.name"),
        "osVersion", System.getProperty("os.version"),
        "javaVersion", System.getProperty("java.version"),
        "javaVendor", System.getProperty("java.vendor")
    ));
    
    return metrics;
  }

  private String formatUptime(long seconds) {
    long days = seconds / 86400;
    long hours = (seconds % 86400) / 3600;
    long minutes = (seconds % 3600) / 60;
    
    StringBuilder sb = new StringBuilder();
    if (days > 0) sb.append(days).append("d ");
    if (hours > 0) sb.append(hours).append("h ");
    if (minutes > 0) sb.append(minutes).append("m");
    if (sb.length() == 0) sb.append(seconds).append("s");
    
    return sb.toString().trim();
  }
}
