package com.datafabric.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datafabric")
public class DatafabricProperties {
  private Path baseDir = Path.of("runtime");
  private Ui ui = new Ui();
  private Reflection reflection = new Reflection();

  public Path getBaseDir() {
    return baseDir;
  }

  public void setBaseDir(Path baseDir) {
    this.baseDir = baseDir;
  }

  public Reflection getReflection() {
    return reflection;
  }

  public void setReflection(Reflection reflection) {
    this.reflection = reflection;
  }

  public Path getResultsDir() {
    return baseDir.resolve("results");
  }

  public Path getAcceleratorDir() {
    return baseDir.resolve("accelerator");
  }

  public Ui getUi() {
    return ui;
  }

  public void setUi(Ui ui) {
    this.ui = ui;
  }

  public static class Ui {
    private String dremioBuildDir = "../dac/ui/build";
    private Map<String, Object> dremioConfig = new LinkedHashMap<>();

    public String getDremioBuildDir() {
      return dremioBuildDir;
    }

    public void setDremioBuildDir(String dremioBuildDir) {
      this.dremioBuildDir = dremioBuildDir;
    }

    public Map<String, Object> getDremioConfig() {
      return dremioConfig;
    }

    public void setDremioConfig(Map<String, Object> dremioConfig) {
      this.dremioConfig = dremioConfig;
    }
  }

  public static class Reflection {
    private int maxCount = 500;
    private boolean maxCountEnabled = true;
    private Duration schedulerDelay = Duration.ofSeconds(30);

    public int getMaxCount() {
      return maxCount;
    }

    public void setMaxCount(int maxCount) {
      this.maxCount = maxCount;
    }

    public boolean isMaxCountEnabled() {
      return maxCountEnabled;
    }

    public void setMaxCountEnabled(boolean maxCountEnabled) {
      this.maxCountEnabled = maxCountEnabled;
    }

    public Duration getSchedulerDelay() {
      return schedulerDelay;
    }

    public void setSchedulerDelay(Duration schedulerDelay) {
      this.schedulerDelay = schedulerDelay;
    }
  }
}
