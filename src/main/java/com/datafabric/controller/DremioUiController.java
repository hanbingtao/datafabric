package com.datafabric.controller;

import com.datafabric.config.DatafabricProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import jakarta.servlet.http.HttpServletRequest;

@RestController
public class DremioUiController {
  private static final String CONFIG_PLACEHOLDER =
      "window.dremioConfig = JSON.parse('${dremio?js_string}');";

  private final DatafabricProperties properties;
  private final ObjectMapper objectMapper;

  public DremioUiController(DatafabricProperties properties, ObjectMapper objectMapper) {
    this.properties = properties;
    this.objectMapper = objectMapper;
  }

  @GetMapping(value = {"/", "/{path:[^\\.]*}", "/**/{path:[^\\.]*}"}, produces = MediaType.TEXT_HTML_VALUE)
  public ResponseEntity<String> index(HttpServletRequest request) throws IOException {
    String uri = request.getRequestURI();
    if (uri.startsWith("/api") || uri.startsWith("/apiv2") || uri.startsWith("/api/v")) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "API endpoint not implemented: " + uri);
    }
    Path indexFile = resolveUiIndexFile();
    // 本地仅启动后端时，允许前端构建产物缺失，并回退到说明页。
    if (!Files.exists(indexFile)) {
      return ResponseEntity.ok()
          .contentType(MediaType.TEXT_HTML)
          .body(localFallbackPage(indexFile));
    }
    String html = Files.readString(indexFile, StandardCharsets.UTF_8);
    return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(injectConfig(html));
  }

  private Path resolveUiIndexFile() {
    return Path.of(properties.getUi().getDremioBuildDir())
        .resolve("index.html")
        .toAbsolutePath()
        .normalize();
  }

  private String injectConfig(String html) throws JsonProcessingException {
    String serialized = objectMapper.writeValueAsString(buildConfig());
    String sourceTypeGlobals =
        """
        <script>
          var UNITY = "UNITY";
          var SNOWFLAKEOPENCATALOG = "SNOWFLAKEOPENCATALOG";
          var REDSHIFT = "REDSHIFT";
          var S3 = "S3";
          var ELASTIC = "ELASTIC";
          var HBASE = "HBASE";
          var HDFS = "HDFS";
          var HIVE = "HIVE";
          var HIVE3 = "HIVE3";
          var MAPRFS = "MAPRFS";
          var SQLSERVER = "MSSQL";
          var MONGODB = "MONGO";
          var MYSQL = "MYSQL";
          var NAS = "NAS";
          var ORACLE = "ORACLE";
          var POSTGRESQL = "POSTGRES";
          var AWSGLUE = "AWSGLUE";
          var GCS = "GCS";
          var AMAZONELASTIC = "AMAZONELASTIC";
          var AZURE_STORAGE = "AZURE_STORAGE";
          var SYNAPSE = "SYNAPSE";
          var ADX = "ADX";
          var MSACCESS = "MSAccess";
          var SPARK = "SPARK";
          var SNOWFLAKE = "SNOWFLAKE";
          var DB2 = "DB2";
          var DREMIOTODREMIO = "DREMIOTODREMIO";
          var DRUID = "DRUID";
          var AZURE_SAMPLE_SOURCE = "SAMPLE_SOURCE";
          var RESTCATALOG = "RESTCATALOG";
          var HOME = "HOME";
          var INTERNAL = "INTERNAL";
          var CASSANDRA = "CASSANDRA";
          var SALESFORCE = "SALESFORCE";
          var NETEZZA = "NETEZZA";
          var TERADATA = "TERADATA";
          var NESSIE = "NESSIE";
          var ARCTIC = "ARCTIC";
          var HISTORYTABLES = "HISTORYTABLES";
          var CLICKHOUSE = "CLICKHOUSE";
        </script>
        """;
    String sessionBootstrap =
        """
        <script>
          if (!window.localStorage.getItem("user")) {
            window.localStorage.setItem("user", JSON.stringify({
              token: "datafabric-local-token",
              userName: "datafabric",
              firstName: "Data",
              lastName: "Fabric",
              expires: %d,
              email: "datafabric@local",
              userId: "00000000-0000-0000-0000-000000000001",
              admin: true,
              createdAt: %d,
              clusterId: "datafabric-cluster",
              clusterCreatedAt: %d,
              version: "0.0.1-SNAPSHOT",
              permissions: {
                canUploadProfiles: true,
                canDownloadProfiles: true,
                canViewJobs: true,
                canCreateSource: true,
                canManageReflections: true
              }
            }));
          }
          if (!window.localStorage.getItem("supportFlags")) {
            window.localStorage.setItem("supportFlags", JSON.stringify({}));
          }
        </script>
        """
            .formatted(
                System.currentTimeMillis() + 30L * 24 * 3600 * 1000,
                System.currentTimeMillis() - 86400000L,
                System.currentTimeMillis() - 7L * 86400000L);
    String websocketCompatibility =
        """
        <script>
          (function() {
            const NativeWebSocket = window.WebSocket;
            if (!NativeWebSocket) return;
            // Dremio UI 依赖 websocket 推送 job 进度，这里用 HTTP 轮询结果做最小兼容。
            class DatafabricSocketCompat {
              constructor(url) {
                this.url = url;
                this.readyState = 0;
                this.protocol = "";
                this.extensions = "";
                this.bufferedAmount = 0;
                this.binaryType = "blob";
                this.onopen = null;
                this.onmessage = null;
                this.onerror = null;
                this.onclose = null;
                this._listeners = { open: [], message: [], error: [], close: [] };
                setTimeout(() => {
                  this.readyState = 1;
                  this._emit("open", { type: "open", target: this, currentTarget: this });
                  this._emit("message", {
                    type: "message",
                    data: JSON.stringify({ type: "connection-established", payload: {} }),
                    target: this,
                    currentTarget: this
                  });
                }, 0);
              }
              send(data) {
                let message;
                try {
                  message = typeof data === "string" ? JSON.parse(data) : data;
                } catch (_e) {
                  return;
                }
                if (!message || !message.type) return;
                if (message.type === "ping") return;
                const jobId = message.payload && message.payload.id;
                if (!jobId) return;
                if (
                  message.type === "job-progress-listen" ||
                  message.type === "qv-job-progress-listen" ||
                  message.type === "job-details-listen" ||
                  message.type === "job-records-listen"
                ) {
                  const detailsUrl = "/apiv2/jobs-listing/v1.0/" + jobId + "/jobDetails?attempt=1&detailLevel=1";
                  fetch(detailsUrl, { credentials: "same-origin" })
                    .then((response) => response.json())
                    .then((details) => {
                      const payload = {
                        id: { id: jobId },
                        update: details,
                        recordCount: details.outputRecords || 0
                      };
                      if (message.type === "job-details-listen") {
                        this._emitMessage({ type: "job-details", payload });
                        return;
                      }
                      if (message.type === "job-records-listen") {
                        this._emitMessage({
                          type: "job-records",
                          payload: { id: { id: jobId }, recordCount: details.outputRecords || 0 }
                        });
                        return;
                      }
                      this._emitMessage({ type: "job-progress", payload });
                      this._emitMessage({ type: "job-progress-newListingUI", payload });
                      this._emitMessage({
                        type: "job-records",
                        payload: { id: { id: jobId }, recordCount: details.outputRecords || 0 }
                      });
                    })
                    .catch(() => {
                      this._emitMessage({
                        type: "job-progress",
                        payload: {
                          id: { id: jobId },
                          update: {
                            id: jobId,
                            state: "FAILED",
                            jobStatus: "FAILED",
                            isComplete: true,
                            failureInfo: {
                              message: "Failed to load job details",
                              errors: [{ message: "Failed to load job details" }]
                            }
                          }
                        }
                      });
                    });
                }
              }
              close(code, reason) {
                this.readyState = 3;
                this._emit("close", {
                  type: "close",
                  code: code || 1000,
                  reason: reason || "",
                  wasClean: true,
                  target: this,
                  currentTarget: this
                });
              }
              addEventListener(type, listener) {
                if (this._listeners[type]) this._listeners[type].push(listener);
              }
              removeEventListener(type, listener) {
                if (!this._listeners[type]) return;
                this._listeners[type] = this._listeners[type].filter((item) => item !== listener);
              }
              dispatchEvent(event) {
                this._emit(event.type, event);
                return true;
              }
              _emit(type, event) {
                const handler = this["on" + type];
                if (typeof handler === "function") handler(event);
                (this._listeners[type] || []).forEach((listener) => listener(event));
              }
              _emitMessage(message) {
                this._emit("message", {
                  type: "message",
                  data: JSON.stringify(message),
                  target: this,
                  currentTarget: this
                });
              }
            }
            window.WebSocket = function(url, protocols) {
              if (typeof url === "string" && url.indexOf("/apiv2/socket") !== -1) {
                return new DatafabricSocketCompat(url, protocols);
              }
              return new NativeWebSocket(url, protocols);
            };
            window.WebSocket.prototype = NativeWebSocket.prototype;
            window.WebSocket.OPEN = NativeWebSocket.OPEN || 1;
            window.WebSocket.CONNECTING = NativeWebSocket.CONNECTING || 0;
            window.WebSocket.CLOSING = NativeWebSocket.CLOSING || 2;
            window.WebSocket.CLOSED = NativeWebSocket.CLOSED || 3;
          })();
        </script>
        """;
    return html.replace(CONFIG_PLACEHOLDER, "window.dremioConfig = " + serialized + ";")
        .replace("</head>", sourceTypeGlobals + sessionBootstrap + websocketCompatibility + "</head>");
  }

  private Map<String, Object> buildConfig() {
    Map<String, Object> config = new LinkedHashMap<>();
    config.put("edition", "OSS");
    config.put("allowDownload", true);
    config.put("allowFileUploads", true);
    config.put("allowSpaceManagement", true);
    config.put("allowSourceManagement", true);
    config.put("serverStatus", "OK");
    config.put("outsideCommunicationDisabled", true);
    config.put("isReleaseBuild", false);
    config.put("crossSourceDisabled", false);
    config.put("queryBundleAdminsEnabled", false);
    config.put("queryBundleUsersEnabled", false);
    config.put("supportEmailTo", "noreply@datafabric.local");
    config.put("supportEmailSubjectForJobs", "");
    config.put("whiteLabelUrl", "datafabric");
    config.put("analyzeTools", Map.of("tableau", Map.of("enabled", false), "powerbi", Map.of("enabled", false)));
    config.putAll(properties.getUi().getDremioConfig());
    return config;
  }

  private String localFallbackPage(Path indexFile) {
    return """
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
          <meta charset="UTF-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1.0" />
          <title>DataFabric Local</title>
          <style>
            :root {
              --bg: #f5f7fb;
              --panel: #ffffff;
              --text: #122033;
              --muted: #617285;
              --line: #d8e1ec;
              --accent: #0f62fe;
            }
            * { box-sizing: border-box; }
            body {
              margin: 0;
              font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
              color: var(--text);
              background: linear-gradient(180deg, #edf4ff 0%%, var(--bg) 100%%);
            }
            main {
              max-width: 860px;
              margin: 48px auto;
              padding: 0 20px;
            }
            section {
              background: var(--panel);
              border: 1px solid var(--line);
              border-radius: 20px;
              padding: 28px;
              box-shadow: 0 16px 40px rgba(18, 32, 51, 0.08);
            }
            h1 { margin: 0 0 12px; font-size: 30px; }
            p { margin: 12px 0; line-height: 1.6; color: var(--muted); }
            code {
              background: #f2f5f8;
              border-radius: 6px;
              padding: 2px 6px;
              word-break: break-all;
            }
            ul { padding-left: 20px; }
            li { margin: 10px 0; line-height: 1.6; color: var(--muted); }
            a { color: var(--accent); text-decoration: none; }
          </style>
        </head>
        <body>
          <main>
            <section>
              <h1>DataFabric 已启动</h1>
              <p>后端服务运行正常，但当前本地没有找到 Dremio UI 的构建产物，所以首页回退到了提示页，而不是完整前端界面。</p>
              <p>期望的 UI 文件位置是：<code>%s</code></p>
              <ul>
                <li>如果你只想验证后端，可以先访问 <a href="/apiv2/server_status">/apiv2/server_status</a>、<a href="/api/v1/metadata/datasets">/api/v1/metadata/datasets</a>、<a href="/api/v3/catalog">/api/v3/catalog</a>。</li>
                <li>如果你需要完整 UI，把前端构建结果放到这个目录，或者把 <code>datafabric.ui.dremio-build-dir</code> 改成正确的 build 目录。</li>
              </ul>
            </section>
          </main>
        </body>
        </html>
        """
        .formatted(escapeHtml(indexFile.toString()));
  }

  private String escapeHtml(String value) {
    return value
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;");
  }
}
