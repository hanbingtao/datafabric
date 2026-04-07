package com.datafabric.service;

import com.datafabric.config.NotificationHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {
  
  private final NotificationHandler notificationHandler;
  private final ObjectMapper objectMapper;
  
  // 通知类型常量
  public static final String TOPIC_JOBS = "jobs";
  public static final String TOPIC_CATALOG = "catalog";
  public static final String TOPIC_REFLECTIONS = "reflections";
  public static final String TOPIC_SYSTEM = "system";
  
  // 通知类型
  public static final String TYPE_JOB_COMPLETED = "JOB_COMPLETED";
  public static final String TYPE_JOB_FAILED = "JOB_FAILED";
  public static final String TYPE_JOB_STARTED = "JOB_STARTED";
  public static final String TYPE_CATALOG_UPDATED = "CATALOG_UPDATED";
  public static final String TYPE_REFLECTION_REFRESHED = "REFLECTION_REFRESHED";
  public static final String TYPE_SYSTEM_ALERT = "SYSTEM_ALERT";
  public static final String TYPE_USER_MESSAGE = "USER_MESSAGE";

  public NotificationService(NotificationHandler notificationHandler, ObjectMapper objectMapper) {
    this.notificationHandler = notificationHandler;
    this.objectMapper = objectMapper;
  }

  public void notifyJobStarted(String jobId, String sql) {
    Notification notification = new Notification();
    notification.type = TYPE_JOB_STARTED;
    notification.topic = TOPIC_JOBS;
    notification.timestamp = Instant.now().toEpochMilli();
    notification.data = Map.of(
        "jobId", jobId,
        "sql", sql,
        "status", "RUNNING"
    );
    sendNotification(notification);
  }

  public void notifyJobCompleted(String jobId, String sql, long rowCount, long durationMs) {
    Notification notification = new Notification();
    notification.type = TYPE_JOB_COMPLETED;
    notification.topic = TOPIC_JOBS;
    notification.timestamp = Instant.now().toEpochMilli();
    notification.data = Map.of(
        "jobId", jobId,
        "sql", sql,
        "status", "COMPLETED",
        "rowCount", rowCount,
        "durationMs", durationMs
    );
    sendNotification(notification);
  }

  public void notifyJobFailed(String jobId, String sql, String errorMessage) {
    Notification notification = new Notification();
    notification.type = TYPE_JOB_FAILED;
    notification.topic = TOPIC_JOBS;
    notification.timestamp = Instant.now().toEpochMilli();
    notification.data = Map.of(
        "jobId", jobId,
        "sql", sql,
        "status", "FAILED",
        "error", errorMessage
    );
    sendNotification(notification);
  }

  public void notifyCatalogUpdated(String entityType, String entityId, String action) {
    Notification notification = new Notification();
    notification.type = TYPE_CATALOG_UPDATED;
    notification.topic = TOPIC_CATALOG;
    notification.timestamp = Instant.now().toEpochMilli();
    notification.data = Map.of(
        "entityType", entityType,
        "entityId", entityId,
        "action", action
    );
    sendNotification(notification);
  }

  public void notifyReflectionRefreshed(String reflectionId, String reflectionName, String status) {
    Notification notification = new Notification();
    notification.type = TYPE_REFLECTION_REFRESHED;
    notification.topic = TOPIC_REFLECTIONS;
    notification.timestamp = Instant.now().toEpochMilli();
    notification.data = Map.of(
        "reflectionId", reflectionId,
        "reflectionName", reflectionName,
        "status", status
    );
    sendNotification(notification);
  }

  public void notifySystemAlert(String alertType, String message, Map<String, Object> details) {
    Notification notification = new Notification();
    notification.type = TYPE_SYSTEM_ALERT;
    notification.topic = TOPIC_SYSTEM;
    notification.timestamp = Instant.now().toEpochMilli();
    notification.data = Map.of(
        "alertType", alertType,
        "message", message,
        "details", details != null ? details : Map.of()
    );
    sendNotification(notification);
  }

  public void notifyUser(String userId, String message, Map<String, Object> data) {
    Notification notification = new Notification();
    notification.type = TYPE_USER_MESSAGE;
    notification.topic = "user:" + userId;
    notification.timestamp = Instant.now().toEpochMilli();
    notification.data = Map.of(
        "message", message,
        "data", data != null ? data : Map.of()
    );
    sendToUser(userId, notification);
  }

  public void broadcastToTopic(String topic, String type, Map<String, Object> data) {
    Notification notification = new Notification();
    notification.type = type;
    notification.topic = topic;
    notification.timestamp = Instant.now().toEpochMilli();
    notification.data = data;
    notificationHandler.broadcastToTopic(topic, toJson(notification));
  }

  private void sendNotification(Notification notification) {
    try {
      String json = toJson(notification);
      notificationHandler.broadcast(json);
      notificationHandler.broadcastToTopic(notification.topic, json);
    } catch (Exception e) {
      System.err.println("Failed to send notification: " + e.getMessage());
    }
  }

  private void sendToUser(String userId, Notification notification) {
    try {
      String json = toJson(notification);
      notificationHandler.sendToUser(userId, json);
    } catch (Exception e) {
      System.err.println("Failed to send notification to user: " + e.getMessage());
    }
  }

  private String toJson(Notification notification) {
    try {
      return objectMapper.writeValueAsString(notification);
    } catch (Exception e) {
      return "{\"type\":\"ERROR\",\"message\":\"Failed to serialize notification\"}";
    }
  }

  public int getActiveConnectionCount() {
    return notificationHandler.getActiveConnectionCount();
  }

  // 通知数据结构
  public static class Notification {
    public String type;
    public String topic;
    public long timestamp;
    public Map<String, Object> data;
  }
}
