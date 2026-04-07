package com.datafabric.service;

import com.datafabric.dto.ScriptListResponse;
import com.datafabric.dto.ScriptRequest;
import com.datafabric.dto.ScriptSummaryResponse;
import com.datafabric.dto.SqlRunnerSessionResponse;
import com.datafabric.model.ScriptRecord;
import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

@Service
public class SqlRunnerCompatibilityService {
  private static final String DEFAULT_USER = "datafabric";
  private static final String DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000001";
  private static final String DEFAULT_SCRIPT_NAME = "__temp_script__";

  private final ConcurrentMap<String, ScriptRecord> scripts = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, SqlRunnerSessionResponse> sessions = new ConcurrentHashMap<>();

  @PostConstruct
  void init() {
    ensureDefaultSession();
  }

  public ScriptListResponse listScripts(int maxResults, String search, String createdBy) {
    ensureDefaultSession();
    String normalizedSearch = search == null ? "" : search.trim().toLowerCase(Locale.ROOT);
    String normalizedCreatedBy = createdBy == null ? "" : createdBy.trim().toLowerCase(Locale.ROOT);

    List<ScriptSummaryResponse> data =
        scripts.values().stream()
            .filter(
                script ->
                    normalizedSearch.isEmpty()
                        || containsIgnoreCase(script.getName(), normalizedSearch)
                        || containsIgnoreCase(script.getContent(), normalizedSearch))
            .filter(
                script ->
                    normalizedCreatedBy.isEmpty()
                        || normalizedCreatedBy.equals(script.getCreatedBy().toLowerCase(Locale.ROOT)))
            .sorted(Comparator.comparing(ScriptRecord::getModifiedAt).reversed())
            .limit(Math.max(1, Math.min(maxResults, 1000)))
            .map(this::toSummary)
            .toList();

    return new ScriptListResponse(data, data.size());
  }

  public ScriptSummaryResponse createScript(ScriptRequest request) {
    ensureDefaultSession();
    Instant now = Instant.now();
    ScriptRecord script = new ScriptRecord();
    script.setId(UUID.randomUUID().toString());
    script.setName(normalizeName(request == null ? null : request.getName(), now));
    script.setContent(request == null || request.getContent() == null ? "" : request.getContent());
    script.setDescription(request == null ? "" : nullToEmpty(request.getDescription()));
    script.setContext(copyList(request == null ? null : request.getContext()));
    script.setJobIds(copyList(request == null ? null : request.getJobIds()));
    script.setCreatedBy(DEFAULT_USER_ID);
    script.setPermissions(new ArrayList<>(List.of("MODIFY", "DELETE", "VIEW")));
    script.setCreatedAt(now);
    script.setModifiedAt(now);
    scripts.put(script.getId(), script);

    SqlRunnerSessionResponse session = getSession();
    List<String> ids = new ArrayList<>(session.getScriptIds());
    ids.add(script.getId());
    session.setScriptIds(ids);
    session.setCurrentScriptId(script.getId());
    return toSummary(script);
  }

  public ScriptSummaryResponse getScript(String scriptId) {
    ensureDefaultSession();
    return toSummary(requireScript(scriptId));
  }

  public ScriptSummaryResponse updateScript(String scriptId, ScriptRequest request) {
    ensureDefaultSession();
    ScriptRecord script = requireScript(scriptId);
    if (request != null) {
      if (request.getName() != null && !request.getName().isBlank()) {
        script.setName(request.getName());
      }
      if (request.getContent() != null) {
        script.setContent(request.getContent());
      }
      if (request.getDescription() != null) {
        script.setDescription(request.getDescription());
      }
      if (request.getContext() != null) {
        script.setContext(copyList(request.getContext()));
      }
      if (request.getJobIds() != null) {
        script.setJobIds(copyList(request.getJobIds()));
      }
    }
    script.setModifiedAt(Instant.now());
    return toSummary(script);
  }

  public ScriptSummaryResponse updateContext(String scriptId, String sessionId) {
    ScriptRecord script = requireScript(scriptId);
    script.setContext(sessionId == null || sessionId.isBlank() ? List.of() : List.of(sessionId));
    script.setModifiedAt(Instant.now());
    return toSummary(script);
  }

  public void deleteScript(String scriptId) {
    ensureDefaultSession();
    ScriptRecord removed = scripts.remove(scriptId);
    if (removed == null) {
      throw new NoSuchElementException("Script not found: " + scriptId);
    }
    SqlRunnerSessionResponse session = getSession();
    List<String> ids =
        session.getScriptIds().stream().filter(existingId -> !existingId.equals(scriptId)).collect(Collectors.toList());
    session.setScriptIds(ids);
    session.setCurrentScriptId(ids.isEmpty() ? null : ids.get(ids.size() - 1));
  }

  public SqlRunnerSessionResponse getSession() {
    return ensureDefaultSession();
  }

  public SqlRunnerSessionResponse updateSession(SqlRunnerSessionResponse request) {
    ensureDefaultSession();
    SqlRunnerSessionResponse session = getSession();
    if (request != null) {
      List<String> validIds =
          copyList(request.getScriptIds()).stream()
              .filter(scripts::containsKey)
              .distinct()
              .collect(Collectors.toList());
      session.setScriptIds(validIds);
      if (request.getCurrentScriptId() != null && validIds.contains(request.getCurrentScriptId())) {
        session.setCurrentScriptId(request.getCurrentScriptId());
      } else {
        session.setCurrentScriptId(validIds.isEmpty() ? null : validIds.get(validIds.size() - 1));
      }
    }
    return session;
  }

  public SqlRunnerSessionResponse addTab(String scriptId) {
    ensureDefaultSession();
    requireScript(scriptId);
    SqlRunnerSessionResponse session = getSession();
    List<String> ids = new ArrayList<>(session.getScriptIds());
    if (!ids.contains(scriptId)) {
      ids.add(scriptId);
    }
    session.setScriptIds(ids);
    session.setCurrentScriptId(scriptId);
    return session;
  }

  public void deleteTab(String scriptId) {
    ensureDefaultSession();
    SqlRunnerSessionResponse session = getSession();
    List<String> ids =
        session.getScriptIds().stream().filter(existingId -> !existingId.equals(scriptId)).collect(Collectors.toList());
    session.setScriptIds(ids);
    session.setCurrentScriptId(ids.isEmpty() ? null : ids.get(ids.size() - 1));
  }

  public Map<String, Object> socketHandshake() {
    return new LinkedHashMap<>(Map.of("status", "READY", "transport", "websocket-disabled"));
  }

  private ScriptRecord requireScript(String scriptId) {
    ScriptRecord script = scripts.get(scriptId);
    if (script == null) {
      throw new NoSuchElementException("Script not found: " + scriptId);
    }
    return script;
  }

  private ScriptSummaryResponse toSummary(ScriptRecord script) {
    ScriptSummaryResponse summary = new ScriptSummaryResponse();
    summary.setId(script.getId());
    summary.setName(script.getName());
    summary.setContent(script.getContent());
    summary.setContext(String.join(".", script.getContext()));
    summary.setContextList(copyList(script.getContext()));
    summary.setCreatedAt(script.getCreatedAt() == null ? null : script.getCreatedAt().toEpochMilli());
    summary.setModifiedAt(script.getModifiedAt() == null ? null : script.getModifiedAt().toEpochMilli());
    summary.setCreatedBy(script.getCreatedBy());
    summary.setJobIds(String.join(",", script.getJobIds()));
    summary.setJobResultUrls(copyList(script.getJobResultUrls()));
    summary.setPermissions(copyList(script.getPermissions()));
    return summary;
  }

  private SqlRunnerSessionResponse ensureDefaultSession() {
    SqlRunnerSessionResponse session =
        sessions.computeIfAbsent(DEFAULT_USER, ignored -> new SqlRunnerSessionResponse());
    if (session.getScriptIds() == null) {
      session.setScriptIds(new ArrayList<>());
    }
    if (session.getScriptIds().isEmpty() || session.getCurrentScriptId() == null) {
      ScriptRecord script = ensureDefaultScript();
      session.setScriptIds(new ArrayList<>(List.of(script.getId())));
      session.setCurrentScriptId(script.getId());
    }
    return session;
  }

  private ScriptRecord ensureDefaultScript() {
    ScriptRecord existing =
        scripts.values().stream().findFirst().orElse(null);
    if (existing != null) {
      if (existing.getPermissions() == null || existing.getPermissions().isEmpty()) {
        existing.setPermissions(new ArrayList<>(List.of("MODIFY", "DELETE", "VIEW")));
      }
      if (existing.getCreatedBy() == null) {
        existing.setCreatedBy(DEFAULT_USER_ID);
      }
      return existing;
    }

    Instant now = Instant.now();
    ScriptRecord script = new ScriptRecord();
    script.setId(UUID.randomUUID().toString());
    script.setName(DEFAULT_SCRIPT_NAME);
    script.setContent("");
    script.setDescription("");
    script.setContext(new ArrayList<>());
    script.setJobIds(new ArrayList<>());
    script.setJobResultUrls(new ArrayList<>());
    script.setPermissions(new ArrayList<>(List.of("MODIFY", "DELETE", "VIEW")));
    script.setCreatedBy(DEFAULT_USER_ID);
    script.setCreatedAt(now);
    script.setModifiedAt(now);
    scripts.put(script.getId(), script);
    return script;
  }

  private static boolean containsIgnoreCase(String source, String search) {
    return source != null && source.toLowerCase(Locale.ROOT).contains(search);
  }

  private static List<String> copyList(List<String> values) {
    return values == null ? new ArrayList<>() : new ArrayList<>(values);
  }

  private static String normalizeName(String name, Instant now) {
    if (name != null && !name.isBlank()) {
      return name;
    }
    return "__DREMIO_TMP__" + now.toString();
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
