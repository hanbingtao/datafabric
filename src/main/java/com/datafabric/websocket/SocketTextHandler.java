package com.datafabric.websocket;

import java.io.IOException;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Component
public class SocketTextHandler extends TextWebSocketHandler {
  @Override
  public void afterConnectionEstablished(WebSocketSession session) throws IOException {
    session.sendMessage(
        new TextMessage(
            """
            {"type":"connection","status":"ok","message":"datafabric socket connected"}
            """.trim()));
  }

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
    session.sendMessage(
        new TextMessage(
            "{\"type\":\"echo\",\"payload\":"
                + quote(message.getPayload())
                + ",\"meta\":{\"source\":\"datafabric\"}}"));
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {}

  private String quote(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
