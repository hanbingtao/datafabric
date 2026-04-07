package com.datafabric.config;

import com.datafabric.websocket.SocketTextHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
  private final SocketTextHandler socketTextHandler;

  public WebSocketConfig(SocketTextHandler socketTextHandler) {
    this.socketTextHandler = socketTextHandler;
  }

  @Override
  public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
    registry.addHandler(socketTextHandler, "/apiv2/socket").setAllowedOrigins("*");
  }
}
