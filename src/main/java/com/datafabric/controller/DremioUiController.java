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
    Path indexFile =
        Path.of(properties.getUi().getDremioBuildDir()).resolve("index.html").toAbsolutePath().normalize();
    String html = Files.readString(indexFile, StandardCharsets.UTF_8);
    return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(injectConfig(html));
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
}
