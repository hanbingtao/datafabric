package com.datafabric.controller;

import com.datafabric.service.SessionService;
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
@RequestMapping({"/api/v3/sessions", "/apiv2/sessions"})
public class SessionController {
  private final SessionService sessionService;

  public SessionController(SessionService sessionService) {
    this.sessionService = sessionService;
  }

  @PostMapping
  public SessionService.SessionRecord createSession(
      @RequestParam(required = false) String userId,
      @RequestBody(required = false) Map<String, Object> context) {
    return sessionService.createSession(userId, context);
  }

  @GetMapping
  public List<SessionService.SessionRecord> listSessions() {
    return sessionService.listSessions();
  }

  @GetMapping("/{sessionId}")
  public SessionService.SessionRecord getSession(@PathVariable String sessionId) {
    return sessionService.getSession(sessionId);
  }

  @GetMapping("/user/{userId}")
  public List<SessionService.SessionRecord> listUserSessions(@PathVariable String userId) {
    return sessionService.listUserSessions(userId);
  }

  @PutMapping("/{sessionId}")
  public SessionService.SessionRecord updateSession(
      @PathVariable String sessionId,
      @RequestBody(required = false) Map<String, Object> context) {
    return sessionService.updateSession(sessionId, context);
  }

  @DeleteMapping("/{sessionId}")
  public Map<String, Object> deleteSession(@PathVariable String sessionId) {
    sessionService.deleteSession(sessionId);
    return Map.of("deleted", true, "sessionId", sessionId);
  }

  @DeleteMapping("/user/{userId}")
  public Map<String, Object> deleteUserSessions(@PathVariable String userId) {
    sessionService.deleteUserSessions(userId);
    return Map.of("deleted", true, "userId", userId);
  }

  @PostMapping("/{sessionId}/refresh")
  public SessionService.SessionRecord refreshSession(@PathVariable String sessionId) {
    sessionService.refreshSession(sessionId);
    return sessionService.getSession(sessionId);
  }

  @GetMapping("/stats")
  public Map<String, Object> getStats() {
    return sessionService.getSessionStats();
  }

  @PostMapping("/cleanup")
  public Map<String, Object> cleanupExpiredSessions() {
    sessionService.cleanupExpiredSessions();
    return Map.of("cleaned", true);
  }
}
