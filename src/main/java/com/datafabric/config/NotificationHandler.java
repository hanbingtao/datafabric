package com.datafabric.config;

import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public class NotificationHandler extends TextWebSocketHandler {
  
  private final CopyOnWriteArraySet<WebSocketSession> sessions = new CopyOnWriteArraySet<>();
  private final Map<String, CopyOnWriteArraySet<WebSocketSession>> topicSessions = new ConcurrentHashMap<>();

  @PostConstruct
  void init() {
    System.out.println("NotificationHandler initialized");
  }

  @Override
  public void afterConnectionEstablished(WebSocketSession session) throws Exception {
    sessions.add(session);
    System.out.println("WebSocket connection established: " + session.getId());
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
    sessions.remove(session);
    for (CopyOnWriteArraySet<WebSocketSession> topicSet : topicSessions.values()) {
      topicSet.remove(session);
    }
    System.out.println("WebSocket connection closed: " + session.getId());
  }

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
    String payload = message.getPayload();
    System.out.println("Received WebSocket message: " + payload);
    
    if ("ping".equals(payload)) {
      session.sendMessage(new TextMessage("pong"));
    }
  }

  @Override
  public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
    System.err.println("WebSocket transport error: " + exception.getMessage());
    sessions.remove(session);
  }

  public void broadcast(String message) {
    for (WebSocketSession session : sessions) {
      try {
        if (session.isOpen()) {
          session.sendMessage(new TextMessage(message));
        }
      } catch (IOException e) {
        System.err.println("Failed to send message: " + e.getMessage());
      }
    }
  }

  public void sendToUser(String userId, String message) {
    broadcast(message);
  }

  public void broadcastToTopic(String topic, String message) {
    CopyOnWriteArraySet<WebSocketSession> topicSubscribers = topicSessions.get(topic);
    if (topicSubscribers != null) {
      for (WebSocketSession session : topicSubscribers) {
        try {
          if (session.isOpen()) {
            session.sendMessage(new TextMessage(message));
          }
        } catch (IOException e) {
          System.err.println("Failed to send topic message: " + e.getMessage());
        }
      }
    }
  }

  public void subscribeToTopic(WebSocketSession session, String topic) {
    topicSessions.computeIfAbsent(topic, k -> new CopyOnWriteArraySet<>()).add(session);
  }

  public void unsubscribeFromTopic(WebSocketSession session, String topic) {
    CopyOnWriteArraySet<WebSocketSession> topicSubscribers = topicSessions.get(topic);
    if (topicSubscribers != null) {
      topicSubscribers.remove(session);
    }
  }

  public int getActiveConnectionCount() {
    int count = 0;
    for (WebSocketSession session : sessions) {
      if (session.isOpen()) {
        count++;
      }
    }
    return count;
  }
}
