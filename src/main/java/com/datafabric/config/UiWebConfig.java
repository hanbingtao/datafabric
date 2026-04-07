package com.datafabric.config;

import java.nio.file.Path;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class UiWebConfig implements WebMvcConfigurer {
  private static final String[] ROOT_ASSETS = {
    "/favicon.ico",
    "/favicon.svg",
    "/apple-touch-icon.png",
    "/manifest.webmanifest",
    "/icon-192.png",
    "/icon-512.png",
    "/editor.worker.js"
  };

  private final DatafabricProperties properties;

  public UiWebConfig(DatafabricProperties properties) {
    this.properties = properties;
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    String buildDir = toLocation(Path.of(properties.getUi().getDremioBuildDir()));
    registry.addResourceHandler("/static/**").addResourceLocations(buildDir + "static/");
    registry.addResourceHandler(ROOT_ASSETS).addResourceLocations(buildDir);
  }

  private String toLocation(Path path) {
    return path.toAbsolutePath().normalize().toUri().toString();
  }
}
