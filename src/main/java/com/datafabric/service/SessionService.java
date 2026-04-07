package com.datafabric.service;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;

@Service
public class SessionService {
  
  private static final String DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000001";
  private static final long SESSION_TIMEOUT_MS = 30 * 60 * 1000; // 30 分钟
  
  private final ConcurrentMap<String, SessionRecord> sessions = new ConcurrentHashMap<>();

  @PostConstruct
  void init() {
    // 创建一个默认会话
    SessionRecord defaultSession = new SessionRecord();
    defaultSession.setId(UUID.randomUUID().toString());
    defaultSession.setUserId(DEFAULT_USER_ID);
    defaultSession.setCreatedAt(Instant.now());
    defaultSession.setLastAccessedAt(Instant.now());
    defaultSession.setActive(true);
    defaultSession.setContext(Map.of("engine", "datafabric", "features", List.of("sql", "transform")));
    sessions.put(defaultSession.getId(), defaultSession);
  }

  public SessionRecord createSession(String userId, Map<String, Object> context) {
    SessionRecord session = new SessionRecord();
    session.setId(UUID.randomUUID().toString());
    session.setUserId(userId != null ? userId : DEFAULT_USER_ID);
    session.setCreatedAt(Instant.now());
    session.setLastAccessedAt(Instant.now());
    session.setActive(true);
    session.setContext(context != null ? context : Map.of());
    sessions.put(session.getId(), session);
    return session;
  }

  public SessionRecord getSession(String sessionId) {
    SessionRecord session = sessions.get(sessionId);
    if (session == null) {
      throw new NoSuchElementException("Session not found: " + sessionId);
    }
    // 更新最后访问时间
    session.setLastAccessedAt(Instant.now());
    return session;
  }

  public List<SessionRecord> listSessions() {
    return sessions.values().stream()
        .sorted((left, right) -> right.getCreatedAt().compareTo(left.getCreatedAt()))
        .toList();
  }

  public List<SessionRecord> listUserSessions(String userId) {
    return sessions.values().stream()
        .filter(s -> s.getUserId().equals(userId))
        .sorted((left, right) -> right.getCreatedAt().compareTo(left.getCreatedAt()))
        .toList();
  }

  public SessionRecord updateSession(String sessionId, Map<String, Object> context) {
    SessionRecord session = getSession(sessionId);
    if (context != null) {
      session.setContext(context);
    }
    session.setLastAccessedAt(Instant.now());
    return session;
  }

  public void deleteSession(String sessionId) {
    sessions.remove(sessionId);
  }

  public void deleteUserSessions(String userId) {
    sessions.entrySet().removeIf(e -> e.getValue().getUserId().equals(userId));
  }

  public void refreshSession(String sessionId) {
    SessionRecord session = getSession(sessionId);
    session.setLastAccessedAt(Instant.now());
  }

  public void cleanupExpiredSessions() {
    long now = Instant.now().toEpochMilli();
    sessions.entrySet().removeIf(e -> 
        (now - e.getValue().getLastAccessedAt().toEpochMilli()) > SESSION_TIMEOUT_MS);
  }

  public Map<String, Object> getSessionStats() {
    long now = Instant.now().toEpochMilli();
    int activeSessions = 0;
    int expiredSessions = 0;
    
    for (SessionRecord session : sessions.values()) {
      if ((now - session.getLastAccessedAt().toEpochMilli()) > SESSION_TIMEOUT_MS) {
        expiredSessions++;
      } else {
        activeSessions++;
      }
    }
    
    return Map.of(
        "total", sessions.size(),
        "active", activeSessions,
        "expired", expiredSessions
    );
  }

  // 内部类
  public static class SessionRecord {
    private String id;
    private String userId;
    private Instant createdAt;
    private Instant lastAccessedAt;
    private boolean active;
    private Map<String, Object> context;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getLastAccessedAt() { return lastAccessedAt; }
    public void setLastAccessedAt(Instant lastAccessedAt) { this.lastAccessedAt = lastAccessedAt; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> context) { this.context = context; }
  }
}
