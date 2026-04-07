package com.datafabric.service;

import com.datafabric.dto.JobAndUserStatsResponse;
import com.datafabric.dto.ApiUserResponse;
import com.datafabric.dto.ScriptListResponse;
import com.datafabric.dto.CatalogItemResponse;
import com.datafabric.dto.CollaborationTagResponse;
import com.datafabric.dto.CollaborationWikiResponse;
import com.datafabric.dto.SettingResponse;
import com.datafabric.dto.SettingsRequest;
import com.datafabric.dto.SettingsWrapperResponse;
import com.datafabric.dto.SourceListResponse;
import com.datafabric.dto.SourceTypeTemplateResponse;
import com.datafabric.dto.UserPreferenceResponse;
import com.datafabric.dto.UserLoginRequest;
import com.datafabric.dto.UserLoginSessionResponse;
import com.datafabric.config.DatafabricProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import com.datafabric.model.JobRecord;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.NoSuchElementException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.stereotype.Service;

@Service
public class BootstrapService {
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final Map<String, CollaborationWikiResponse> WIKI_STORE = new ConcurrentHashMap<>();
  private static final Map<String, CollaborationTagResponse> TAG_STORE = new ConcurrentHashMap<>();
  private static final Map<String, SpaceRecord> SPACES = new ConcurrentHashMap<>();
  private static final Map<String, SourceRecord> SOURCES = new ConcurrentHashMap<>();
  private static final Map<String, SourceRecord> SOURCES_BY_NAME = new ConcurrentHashMap<>();
  private static final Map<String, FolderRecord> FOLDERS = new ConcurrentHashMap<>();
  private static final Map<String, FileFormatRecord> FILE_FORMATS = new ConcurrentHashMap<>();
  private static final TypeReference<SourceRecord> SOURCE_RECORD_TYPE = new TypeReference<>() {};

  private final JobService jobService;
  private final MetadataService metadataService;
  private final ClickHouseSourceService clickHouseSourceService;
  private final DatafabricProperties properties;
  private final ObjectMapper objectMapper;
  private final UserPreferenceService userPreferenceService;

  public BootstrapService(
      JobService jobService,
      MetadataService metadataService,
      ClickHouseSourceService clickHouseSourceService,
      DatafabricProperties properties,
      ObjectMapper objectMapper,
      UserPreferenceService userPreferenceService) {
    this.jobService = jobService;
    this.metadataService = metadataService;
    this.clickHouseSourceService = clickHouseSourceService;
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.userPreferenceService = userPreferenceService;
  }

  @PostConstruct
  void loadPersistedState() throws IOException {
    Files.createDirectories(sourceStoreDir());
    for (Path file : listSourceFiles()) {
      SourceRecord record = objectMapper.readValue(Files.newInputStream(file), SOURCE_RECORD_TYPE);
      SOURCES.put(record.id(), record);
      SOURCES_BY_NAME.put(record.name().toLowerCase(), record);
    }
  }

  public UserLoginSessionResponse getDefaultSession() {
    UserLoginSessionResponse session = new UserLoginSessionResponse();
    session.setToken("datafabric-local-token");
    session.setUserName("datafabric");
    session.setFirstName("Data");
    session.setLastName("Fabric");
    session.setEmail("datafabric@local");
    session.setUserId("00000000-0000-0000-0000-000000000001");
    session.setAdmin(true);
    session.setCreatedAt(Instant.now().minusSeconds(86400).toEpochMilli());
    session.setExpires(Instant.now().plusSeconds(86400 * 30L).toEpochMilli());
    session.setClusterId("datafabric-cluster");
    session.setClusterCreatedAt(Instant.now().minusSeconds(86400 * 7L).toEpochMilli());
    session.setVersion("0.0.1-SNAPSHOT");
    session.setPermissions(new UserLoginSessionResponse.SessionPermissions());
    return session;
  }

  public UserLoginSessionResponse login(UserLoginRequest request) {
    return getDefaultSession();
  }

  public SourceListResponse listSources(boolean includeDatasetCount) {
    List<SourceListResponse.SourceInfo> allSources = new ArrayList<>();
    allSources.add(toSourceInfo(defaultSampleSource(), includeDatasetCount));
    allSources.addAll(
        SOURCES.values().stream()
            .sorted((left, right) -> left.createdAt().compareTo(right.createdAt()))
            .map(source -> toSourceInfo(source, includeDatasetCount))
            .toList());
    return new SourceListResponse(allSources);
  }

  public Map<String, Object> isMetadataImpacting(Map<String, Object> request) {
    // 当前后端还没有真实元数据血缘和反射清理能力，这里先兼容前端预检查接口，
    // 统一按“非 metadata impacting”返回，保证编辑 source 的主流程可继续执行。
    return Map.of("isMetadataImpacting", false);
  }

  public Map<String, Object> getServerStatus() {
    return Map.of("status", "OK");
  }

  public Map<String, Object> getSystemNodes() {
    long currentTime = Instant.now().toEpochMilli();
    Map<String, Object> node =
        new LinkedHashMap<>();
    node.put("name", hostName());
    node.put("host", hostName());
    node.put("ip", "127.0.0.1");
    node.put("port", 39047);
    node.put("isCoordinator", true);
    node.put("isExecutor", true);
    node.put("isMaster", true);
    node.put("isCompatible", true);
    node.put("status", "green");
    node.put("state", "RUNNING");
    node.put("version", "0.0.1-SNAPSHOT");
    node.put("start", currentTime - ManagementFactory.getRuntimeMXBean().getUptime());
    node.put("uptime", ManagementFactory.getRuntimeMXBean().getUptime());
    node.put("cpu", 0.12d);
    node.put("memory", Map.of("heapUsed", 0L, "heapMax", 0L, "directCurrent", 0L, "directMax", 0L));
    node.put("roles", List.of("MASTER", "COORDINATOR", "EXECUTOR"));
    node.put("tag", "datafabric-node");
    return Map.of("nodes", List.of(node));
  }

  private String hostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      return "localhost";
    }
  }

  public Map<String, Object> getHome(String homeName, boolean includeContents) {
    String normalized = homeName == null ? "@datafabric" : homeName;
    if (!"@datafabric".equalsIgnoreCase(normalized)) {
      throw new NoSuchElementException("Home not found: " + homeName);
    }

    Map<String, Object> links =
        Map.of(
            "self", "/home/%40datafabric",
            "jobs", "/jobs?filters=qt==UI,qt==EXTERNAL;contains=@datafabric",
            "file_format", "/home/%40datafabric/file_format",
            "file_prefix", "/home/%40datafabric/file",
            "upload_start", "/home/%40datafabric/upload_start");

    Map<String, Object> home =
        new LinkedHashMap<>(
            Map.of(
                "id", "11111111-1111-1111-1111-111111111111",
                "name", "@datafabric",
                "owner", "datafabric",
                "fullPathList", List.of("@datafabric"),
                "resourcePath", "/home/%40datafabric",
                "description", "Personal space for user datafabric",
                "ctime", Instant.now().minusSeconds(86400).toEpochMilli(),
                "links", links));

    if (includeContents) {
      home.put("contents", folderContents("home", normalized, null));
    }

    return home;
  }

  public Map<String, Object> getSource(String sourceName, boolean includeContents) {
    SourceRecord sourceRecord = findSourceByName(sourceName);
    Map<String, Object> source = sourceEntity(sourceRecord);
    if (includeContents) {
      source.put("contents", folderContents("source", sourceName, null));
    }
    return source;
  }

  public Map<String, Object> saveSource(String sourceName, Map<String, Object> body) {
    String requestedName = normalizeString(body.get("name"));
    String finalName = requestedName.isBlank() ? sourceName : requestedName;
    String type = normalizeString(body.get("type"));
    if (finalName.isBlank()) {
      throw new IllegalArgumentException("Source name is required");
    }
    if (type.isBlank()) {
      throw new IllegalArgumentException("Source type is required");
    }

    SourceRecord existing = findSourceByNameOrNull(sourceName);
    ensureSourceNameAvailable(finalName, existing == null ? null : existing.id());

    Instant createdAt = existing == null ? Instant.now() : existing.createdAt();
    String id = existing == null ? UUID.randomUUID().toString() : existing.id();
    String tag = "source-" + Instant.now().toEpochMilli();
    String description =
        body != null && body.containsKey("description")
            ? normalizeString(body.get("description"))
            : existing == null ? "" : existing.description();
    Map<String, Object> config = extractMap(body.get("config"));
    Map<String, Object> metadataPolicy = extractMap(body.get("metadataPolicy"));

    SourceRecord updated =
        new SourceRecord(
            id,
            finalName,
            type,
            description,
            tag,
            createdAt,
            config,
            metadataPolicy);
    if (existing != null) {
      SOURCES.remove(existing.id());
      SOURCES_BY_NAME.remove(existing.name().toLowerCase());
    }
    SOURCES.put(updated.id(), updated);
    SOURCES_BY_NAME.put(updated.name().toLowerCase(), updated);
    savePersistedSource(updated);
    return sourceEntity(updated);
  }

  public void deleteSource(String sourceName, String version) {
    SourceRecord existing = findSourceByName(sourceName);
    if (version != null && !version.isBlank() && !Objects.equals(version, existing.tag())) {
      throw new IllegalArgumentException("Source version does not match current tag");
    }
    SOURCES.remove(existing.id());
    SOURCES_BY_NAME.remove(existing.name().toLowerCase());
    deletePersistedSource(existing.name());
  }

  public Map<String, Object> renameSource(String sourceName, String renameTo) {
    SourceRecord existing = findSourceByName(sourceName);
    String newName = normalizeString(renameTo);
    if (newName.isBlank()) {
      throw new IllegalArgumentException("Source name is required");
    }
    ensureSourceNameAvailable(newName, existing.id());
    SourceRecord updated =
        new SourceRecord(
            existing.id(),
            newName,
            existing.type(),
            existing.description(),
            "source-" + Instant.now().toEpochMilli(),
            existing.createdAt(),
            existing.config(),
            existing.metadataPolicy());
    SOURCES.put(updated.id(), updated);
    SOURCES_BY_NAME.remove(existing.name().toLowerCase());
    SOURCES_BY_NAME.put(updated.name().toLowerCase(), updated);
    deletePersistedSource(existing.name());
    savePersistedSource(updated);
    return sourceEntity(updated);
  }

  public Map<String, Object> getLegacyUser(String userName) {
    ApiUserResponse user = getUserByName(userName);
    return new LinkedHashMap<>(
        Map.of(
            "id", user.getId(),
            "name", user.getName(),
            "userName", user.getName(),
            "firstName", user.getFirstName(),
            "lastName", user.getLastName(),
            "email", user.getEmail(),
            "tag", user.getTag(),
            "active", Boolean.TRUE.equals(user.getActive())));
  }

  public Map<String, Object> getFolder(
      String rootType, String rootName, String path, boolean includeContents) {
    if ("source".equals(rootType) && clickHouseSourceService.isClickHouseSource(rootName)) {
      return clickHouseFolderEntity(rootName, path, includeContents);
    }
    FolderRecord folder = requireFolder(rootType, rootName, path);
    return folderEntity(folder, includeContents);
  }

  public Map<String, Object> createFolder(
      String rootType, String rootName, String parentPath, Map<String, Object> request) {
    validateRoot(rootType, rootName);
    String folderName = normalizeString(request.get("name"));
    if (folderName.isBlank()) {
      throw new IllegalArgumentException("Folder name is required");
    }
    String normalizedParent = normalizePath(parentPath);
    String fullPath = normalizedParent.isBlank() ? folderName : normalizedParent + "/" + folderName;
    String key = folderKey(rootType, rootName, fullPath);
    if (FOLDERS.containsKey(key)) {
      throw new IllegalArgumentException("Folder already exists: " + fullPath);
    }
    FolderRecord folder =
        new FolderRecord(
            UUID.randomUUID().toString(),
            rootType,
            rootName,
            fullPath,
            folderName,
            "folder-" + Instant.now().toEpochMilli(),
            Instant.now(),
            normalizeString(request.get("storageUri")));
    FOLDERS.put(key, folder);
    return folderEntity(folder, false);
  }

  public Map<String, Object> updateFolder(
      String rootType, String rootName, String path, Map<String, Object> request) {
    FolderRecord existing = requireFolder(rootType, rootName, path);
    String requestedName = normalizeString(request.get("name"));
    String nextName = requestedName.isBlank() ? existing.name() : requestedName;
    String nextStorageUri =
        request.containsKey("storageUri")
            ? normalizeString(request.get("storageUri"))
            : existing.storageUri();
    String parentPath = parentPath(existing.relativePath());
    String nextRelativePath = parentPath.isBlank() ? nextName : parentPath + "/" + nextName;
    renameFolderTree(existing, nextRelativePath, nextName, nextStorageUri);
    return folderEntity(requireFolder(rootType, rootName, nextRelativePath), false);
  }

  public void deleteFolder(String rootType, String rootName, String path, String version) {
    FolderRecord existing = requireFolder(rootType, rootName, path);
    if (version != null && !version.isBlank() && !Objects.equals(version, existing.tag())) {
      throw new IllegalArgumentException("Folder version does not match current tag");
    }
    String prefix = folderKey(rootType, rootName, existing.relativePath()) + "/";
    boolean hasChildren = FOLDERS.keySet().stream().anyMatch(key -> key.startsWith(prefix));
    if (hasChildren) {
      throw new IllegalArgumentException("nessie_catalog:folder_is_not_empty");
    }
    FOLDERS.remove(folderKey(rootType, rootName, existing.relativePath()));
  }

  public Map<String, Object> getFileFormat(
      String rootType, String rootName, String path, boolean folderFormat) {
    validateRoot(rootType, rootName);
    return fileFormatEntity(fileFormatRecord(rootType, rootName, path, folderFormat));
  }

  public Map<String, Object> saveFileFormat(
      String rootType,
      String rootName,
      String path,
      boolean folderFormat,
      Map<String, Object> request) {
    validateRoot(rootType, rootName);
    FileFormatRecord existing = fileFormatRecord(rootType, rootName, path, folderFormat);
    String type = normalizeString(request.get("type"));
    if (type.isBlank()) {
      type = existing.type();
    }
    FileFormatRecord updated =
        new FileFormatRecord(
            existing.id(),
            rootType,
            rootName,
            normalizePath(path),
            folderFormat,
            type.isBlank() ? "JSON" : type,
            "format-" + Instant.now().toEpochMilli(),
            mergeMaps(existing.payload(), request));
    FILE_FORMATS.put(fileFormatKey(rootType, rootName, path, folderFormat), updated);
    return fileFormatEntity(updated);
  }

  public void deleteFileFormat(
      String rootType, String rootName, String path, boolean folderFormat, String version) {
    FileFormatRecord existing = fileFormatRecord(rootType, rootName, path, folderFormat);
    if (version != null && !version.isBlank() && !Objects.equals(version, existing.version())) {
      throw new IllegalArgumentException("File format version does not match current version");
    }
    FILE_FORMATS.remove(fileFormatKey(rootType, rootName, path, folderFormat));
  }

  public Map<String, Object> previewFileFormat(
      String rootType, String rootName, String path, Map<String, Object> request) {
    validateRoot(rootType, rootName);
    String location =
        normalizePath(path).isBlank() ? rootName : rootName + "/" + normalizePath(path);
    return Map.of(
        "columns",
        List.of(
            Map.of("name", "path", "type", "VARCHAR"),
            Map.of("name", "type", "type", "VARCHAR"),
            Map.of("name", "preview", "type", "VARCHAR")),
        "rows",
        List.of(
            Map.of(
                "path", location,
                "type", normalizeString(request.get("type")).isBlank() ? "JSON" : normalizeString(request.get("type")),
                "preview", "datafabric preview")),
        "rowCount",
        1);
  }

  public Map<String, Object> getSpace(String spaceName, boolean includeContents) {
    SpaceRecord space =
        SPACES.values().stream()
            .filter(item -> item.name().equalsIgnoreCase(spaceName))
            .findFirst()
            .orElseThrow(() -> new NoSuchElementException("Space not found: " + spaceName));

    Map<String, Object> entity = new LinkedHashMap<>(spaceEntity(space, 100));
    if (!includeContents) {
      entity.remove("contents");
    }
    return entity;
  }

  public Map<String, Object> getResourceTree(
      boolean showDatasets, boolean showSources, boolean showSpaces, boolean showHomes) {
    List<Map<String, Object>> resources = new ArrayList<>();
    if (showSources) {
      resources.add(
          new LinkedHashMap<>(
              Map.of(
                  "id", samplesSourceCatalogItem().getId(),
                  "type", "SOURCE",
                  "name", "Samples",
                  "fullPath", List.of("Samples"),
                  "rootType", "SOURCE",
                  "state", Map.of("status", "good", "suggestedUserAction", "", "messages", List.of()))));
    }
    if (showSpaces) {
      // 添加所有 Space
      for (SpaceRecord space : SPACES.values()) {
        resources.add(
            new LinkedHashMap<>(
                Map.of(
                    "id", space.id(),
                    "type", "SPACE",
                    "name", space.name(),
                    "fullPath", List.of(space.name()),
                    "rootType", "SPACE")));
      }
    }
    if (showHomes) {
      resources.add(
          new LinkedHashMap<>(
              Map.of(
                  "id", "11111111-1111-1111-1111-111111111111",
                  "type", "HOME",
                  "name", "@datafabric",
                  "fullPath", List.of("@datafabric"),
                  "url", "/resourcetree/%22%40datafabric%22",
                  "rootType", "HOME")));
    }
    return Map.of("resources", resources);
  }

  public Map<String, Object> getResourceTreePath(
      String rootPath, boolean showDatasets, boolean showSources, boolean showSpaces, boolean showHomes) {
    if ("Samples".equalsIgnoreCase(rootPath)) {
      List<Map<String, Object>> resources = new ArrayList<>();
      if (showDatasets) {
        resources.add(
            new LinkedHashMap<>(
                Map.of(
                    "id", "22222222-2222-2222-2222-222222222222",
                    "type", "PHYSICAL_DATASET",
                    "name", "SALES_FACT",
                    "fullPath", List.of("Samples", "SALES_FACT"),
                    "rootType", "SOURCE",
                    "metadataOutdated", false)));
      }
      return Map.of("resources", resources);
    }

    if ("%22%40datafabric%22".equalsIgnoreCase(rootPath) || "\"@datafabric\"".equalsIgnoreCase(rootPath)) {
      return Map.of("resources", List.of());
    }

    return Map.of("resources", List.of());
  }

  public Map<String, Object> listSqlFunctions() {
    List<Map<String, Object>> functions = new ArrayList<>();
    
    // 添加基础 SQL 聚合函数
    addFunction(functions, "COUNT", "AGGREGATE", "COUNT(*)", "COUNT(DISTINCT expression)", "COUNT(expression)");
    addFunction(functions, "SUM", "AGGREGATE", "SUM(expression)", "SUM(DISTINCT expression)", "");
    addFunction(functions, "AVG", "AGGREGATE", "AVG(expression)", "AVG(DISTINCT expression)", "");
    addFunction(functions, "MIN", "AGGREGATE", "MIN(expression)", "", "");
    addFunction(functions, "MAX", "AGGREGATE", "MAX(expression)", "", "");
    addFunction(functions, "GROUP_CONCAT", "AGGREGATE", "GROUP_CONCAT(expression, separator)", "", "");
    addFunction(functions, "LISTAGG", "AGGREGATE", "LISTAGG(expression, separator)", "", "");
    
    // 添加字符串函数
    addFunction(functions, "CONCAT", "STRING", "CONCAT(string1, string2)", "", "");
    addFunction(functions, "LENGTH", "STRING", "LENGTH(string)", "", "");
    addFunction(functions, "UPPER", "STRING", "UPPER(string)", "", "");
    addFunction(functions, "LOWER", "STRING", "LOWER(string)", "", "");
    addFunction(functions, "TRIM", "STRING", "TRIM(string)", "", "");
    addFunction(functions, "LTRIM", "STRING", "LTRIM(string)", "", "");
    addFunction(functions, "RTRIM", "STRING", "RTRIM(string)", "", "");
    addFunction(functions, "SUBSTRING", "STRING", "SUBSTRING(string, start, length)", "", "");
    addFunction(functions, "REPLACE", "STRING", "REPLACE(string, from, to)", "", "");
    addFunction(functions, "REGEXP_MATCHES", "STRING", "REGEXP_MATCHES(string, pattern)", "", "");
    addFunction(functions, "REGEXP_REPLACE", "STRING", "REGEXP_REPLACE(string, pattern, replacement)", "", "");
    addFunction(functions, "SPLIT", "STRING", "SPLIT(string, delimiter)", "", "");
    addFunction(functions, "INITCAP", "STRING", "INITCAP(string)", "", "");
    addFunction(functions, "LPAD", "STRING", "LPAD(string, length, pad)", "", "");
    addFunction(functions, "RPAD", "STRING", "RPAD(string, length, pad)", "", "");
    
    // 添加日期时间函数
    addFunction(functions, "CURRENT_DATE", "DATE", "CURRENT_DATE", "", "");
    addFunction(functions, "CURRENT_TIMESTAMP", "DATE", "CURRENT_TIMESTAMP", "", "");
    addFunction(functions, "DATE_ADD", "DATE", "DATE_ADD(date, interval)", "", "");
    addFunction(functions, "DATE_SUB", "DATE", "DATE_SUB(date, interval)", "", "");
    addFunction(functions, "DATE_DIFF", "DATE", "DATE_DIFF(date1, date2)", "", "");
    addFunction(functions, "EXTRACT", "DATE", "EXTRACT(unit FROM date)", "", "");
    addFunction(functions, "NOW", "DATE", "NOW()", "", "");
    addFunction(functions, "TO_DATE", "DATE", "TO_DATE(string, format)", "", "");
    addFunction(functions, "TO_CHAR", "DATE", "TO_CHAR(date, format)", "", "");
    addFunction(functions, "DAY", "DATE", "DAY(date)", "", "");
    addFunction(functions, "MONTH", "DATE", "MONTH(date)", "", "");
    addFunction(functions, "YEAR", "DATE", "YEAR(date)", "", "");
    addFunction(functions, "WEEK", "DATE", "WEEK(date)", "", "");
    addFunction(functions, "QUARTER", "DATE", "QUARTER(date)", "", "");
    
    // 添加数学函数
    addFunction(functions, "ABS", "NUMERIC", "ABS(number)", "", "");
    addFunction(functions, "ROUND", "NUMERIC", "ROUND(number, scale)", "", "");
    addFunction(functions, "FLOOR", "NUMERIC", "FLOOR(number)", "", "");
    addFunction(functions, "CEIL", "NUMERIC", "CEIL(number)", "", "");
    addFunction(functions, "SQRT", "NUMERIC", "SQRT(number)", "", "");
    addFunction(functions, "POWER", "NUMERIC", "POWER(number, exponent)", "", "");
    addFunction(functions, "MOD", "NUMERIC", "MOD(number, divisor)", "", "");
    addFunction(functions, "LOG", "NUMERIC", "LOG(number, base)", "", "");
    addFunction(functions, "LN", "NUMERIC", "LN(number)", "", "");
    addFunction(functions, "EXP", "NUMERIC", "EXP(number)", "", "");
    
    // 添加条件函数
    addFunction(functions, "CASE", "CONDITIONAL", "CASE WHEN condition THEN result ... END", "", "");
    addFunction(functions, "COALESCE", "CONDITIONAL", "COALESCE(value, ...)", "", "");
    addFunction(functions, "NULLIF", "CONDITIONAL", "NULLIF(value1, value2)", "", "");
    addFunction(functions, "IFNULL", "CONDITIONAL", "IFNULL(value, replacement)", "", "");
    addFunction(functions, "IF", "CONDITIONAL", "IF(condition, then, else)", "", "");
    
    // 添加类型转换函数
    addFunction(functions, "CAST", "CAST", "CAST(expression AS type)", "", "");
    addFunction(functions, "TRY_CAST", "CAST", "TRY_CAST(expression AS type)", "", "");
    addFunction(functions, "TO_VARCHAR", "CAST", "TO_VARCHAR(expression, format)", "", "");
    addFunction(functions, "TO_NUMBER", "CAST", "TO_NUMBER(string)", "", "");
    addFunction(functions, "TO_INTEGER", "CAST", "TO_INTEGER(expression)", "", "");
    addFunction(functions, "TO_BIGINT", "CAST", "TO_BIGINT(expression)", "", "");
    addFunction(functions, "TO_FLOAT", "CAST", "TO_FLOAT(expression)", "", "");
    addFunction(functions, "TO_DOUBLE", "CAST", "TO_DOUBLE(expression)", "", "");
    addFunction(functions, "TO_BOOLEAN", "CAST", "TO_BOOLEAN(expression)", "", "");
    
    return Map.of("functions", functions);
  }
  
  private void addFunction(List<Map<String, Object>> functions, String name, String category, String syntax, String distinctSyntax, String alternativeSyntax) {
    Map<String, Object> func = new LinkedHashMap<>();
    func.put("name", name);
    func.put("category", category);
    func.put("syntax", syntax);
    if (distinctSyntax != null && !distinctSyntax.isBlank()) {
      func.put("distinctSyntax", distinctSyntax);
    }
    if (alternativeSyntax != null && !alternativeSyntax.isBlank()) {
      func.put("alternativeSyntax", alternativeSyntax);
    }
    functions.add(func);
  }

  public Map<String, Object> getDefaultTreeReference() {
    return Map.of(
        "reference",
        Map.of(
            "type", "BRANCH",
            "name", "main",
            "hash", "00000000000000000000000000000000"));
  }

  public Map<String, Object> listTreeReferences() {
    return Map.of(
        "references",
        List.of(
            Map.of(
                "type", "BRANCH",
                "name", "main",
                "hash", "00000000000000000000000000000000")),
        "hasMore",
        false);
  }

  public Map<String, Object> getSupportFlag(String key) {
    return Map.of("id", key, "type", "BOOLEAN", "value", false);
  }

  public SettingsWrapperResponse listSettings(SettingsRequest request) {
    Set<String> required = new LinkedHashSet<>();
    if (request != null && request.getRequiredSettings() != null) {
      required.addAll(request.getRequiredSettings());
    }

    if (request != null && request.isIncludeSetSettings()) {
      required.addAll(defaultSupportFlagKeys());
    }

    return new SettingsWrapperResponse(
        required.stream().map(this::supportFlagResponse).toList());
  }

  public List<CatalogItemResponse> listCatalogRoot() {
    List<CatalogItemResponse> root = new ArrayList<>();
    root.add(homeCatalogItem());
    // 前端根目录依赖 HOME/SPACE/SOURCE 混合返回，这里把内存中的空间节点也挂进去。
    root.addAll(SPACES.values().stream()
        .sorted((left, right) -> left.createdAt().compareTo(right.createdAt()))
        .map(this::spaceCatalogItem)
        .toList());
    root.add(samplesSourceCatalogItem());
    root.addAll(SOURCES.values().stream()
        .sorted((left, right) -> left.createdAt().compareTo(right.createdAt()))
        .map(this::sourceCatalogItem)
        .toList());
    return root;
  }

  public Map<String, Object> getCatalogById(String id, Integer maxChildren) {
    if (samplesSourceCatalogItem().getId().equals(id)) {
      return samplesSourceEntity(maxChildren);
    }
    SourceRecord source = SOURCES.get(id);
    if (source != null) {
      return sourceCatalogEntity(source, maxChildren);
    }
    if (homeCatalogItem().getId().equals(id)) {
      return homeEntity(maxChildren);
    }
    SpaceRecord space = SPACES.get(id);
    if (space != null) {
      return spaceEntity(space, maxChildren);
    }
    if (salesFactDatasetEntity().get("id").equals(id)) {
      return salesFactDatasetEntity();
    }
    throw new NoSuchElementException("Catalog entity not found: " + id);
  }

  public Map<String, Object> getCatalogByPath(List<String> path, Integer maxChildren) {
    if (path == null || path.isEmpty()) {
      throw new NoSuchElementException("Catalog path is empty");
    }
    if (path.size() == 1 && "Samples".equalsIgnoreCase(path.get(0))) {
      return samplesSourceEntity(maxChildren);
    }
    if (path.size() == 1) {
      SourceRecord source = findSourceByNameOrNull(path.get(0));
      if (source != null) {
        return sourceCatalogEntity(source, maxChildren);
      }
    }
    if (path.size() == 1 && "@datafabric".equalsIgnoreCase(path.get(0))) {
      return homeEntity(maxChildren);
    }
    if (path.size() == 1) {
      return SPACES.values().stream()
          .filter(space -> space.name().equalsIgnoreCase(path.get(0)))
          .findFirst()
          .map(space -> spaceEntity(space, maxChildren))
          .orElseGet(() -> missingCatalogPath(path));
    }
    if (path.size() == 2
        && "Samples".equalsIgnoreCase(path.get(0))
        && "SALES_FACT".equalsIgnoreCase(path.get(1))) {
      return salesFactDatasetEntity();
    }
    return missingCatalogPath(path);
  }

  public Map<String, Object> createCatalog(Map<String, Object> request) {
    String entityType = normalizeString(request.get("entityType"));
    if (!"space".equalsIgnoreCase(entityType)) {
      throw new IllegalArgumentException("Only entityType=space is currently supported");
    }

    String name = normalizeString(request.get("name"));
    if (name.isBlank()) {
      throw new IllegalArgumentException("Space name is required");
    }
    if (name.startsWith("@")) {
      throw new IllegalArgumentException("Space name cannot start with '@'");
    }
    ensureSpaceNameAvailable(name, null);

    Instant now = Instant.now();
    SpaceRecord space =
        new SpaceRecord(
            UUID.randomUUID().toString(),
            name,
            normalizeString(request.get("description")),
            "space-" + now.toEpochMilli(),
            now);
    // 当前只做单机演示，因此空间元数据保存在进程内存中。
    SPACES.put(space.id(), space);
    return spaceEntity(space, 100);
  }

  public Map<String, Object> updateCatalog(String id, Map<String, Object> request) {
    SpaceRecord existing = requireSpace(id);
    String requestedName = normalizeString(request.get("name"));
    String name = requestedName.isBlank() ? existing.name() : requestedName;
    ensureSpaceNameAvailable(name, id);

    String description =
        request != null && request.containsKey("description")
            ? normalizeString(request.get("description"))
            : existing.description();

    SpaceRecord updated =
        new SpaceRecord(
            existing.id(),
            name,
            description,
            "space-" + Instant.now().toEpochMilli(),
            existing.createdAt());
    SPACES.put(updated.id(), updated);
    return spaceEntity(updated, 100);
  }

  public void deleteCatalog(String id) {
    if (SPACES.remove(id) != null) {
      return;
    }
    throw new NoSuchElementException("Catalog entity not found: " + id);
  }

  public ScriptListResponse listScripts(int maxResults, String search, String createdBy) {
    return new ScriptListResponse(List.of(), 0);
  }

  public List<SourceTypeTemplateResponse> listSourceTypes() {
    return List.of(
        sourceType("S3", "Amazon S3", "sources/S3", true, false),
        sourceType("HIVE", "Hive 2.x", "sources/Hive", true, false),
        sourceType("HIVE3", "Hive 3.x", "sources/Hive", true, false),
        sourceType("HDFS", "HDFS", "sources/HDFS", true, false),
        sourceType("NAS", "NAS", "sources/NAS", true, false),
        sourceType("MYSQL", "MySQL", "sources/MySQL", true, false),
        sourceType("POSTGRES", "PostgreSQL", "sources/Postgres", true, false),
        sourceType("NESSIE", "Nessie", "sources/Nessie", false, false),
        sourceType("ARCTIC", "Arctic", "sources/ArcticCatalog", false, false),
        sourceType("CLICKHOUSE", "ClickHouse", "sources/Clickhouse", true, false));
  }

  public SourceTypeTemplateResponse getSourceType(String name) {
    return listSourceTypes().stream()
        .filter(item -> item.getSourceType().equalsIgnoreCase(name))
        .findFirst()
        .map(this::withUiConfig)
        .orElseThrow(() -> new NoSuchElementException("Source type not found: " + name));
  }

  public ApiUserResponse getUserByName(String name) {
    if (!"datafabric".equalsIgnoreCase(name)) {
      throw new NoSuchElementException("User not found: " + name);
    }
    ApiUserResponse user = new ApiUserResponse();
    user.setId("00000000-0000-0000-0000-000000000001");
    user.setName("datafabric");
    user.setFirstName("Data");
    user.setLastName("Fabric");
    user.setEmail("datafabric@local");
    user.setTag("datafabric-user-tag");
    user.setExtra("{}");
    user.setActive(true);
    return user;
  }

  public UserPreferenceResponse getPreference(String preferenceType, boolean showCatalogInfo) {
    return userPreferenceService.getPreference(preferenceType, showCatalogInfo);
  }

  public UserPreferenceResponse addPreference(String preferenceType, String entityId) {
    return userPreferenceService.addPreference(preferenceType, entityId);
  }

  public UserPreferenceResponse removePreference(String preferenceType, String entityId) {
    return userPreferenceService.removePreference(preferenceType, entityId);
  }

  public CollaborationWikiResponse getWiki(String id) {
    return WIKI_STORE.computeIfAbsent(id, key -> defaultWiki());
  }

  public CollaborationWikiResponse saveWiki(String id, CollaborationWikiResponse wiki) {
    long nextVersion = getWiki(id).getVersion() == null ? 0L : getWiki(id).getVersion() + 1L;
    CollaborationWikiResponse updated =
        new CollaborationWikiResponse(wiki == null ? "" : wiki.getText(), nextVersion);
    WIKI_STORE.put(id, updated);
    return updated;
  }

  public CollaborationTagResponse getTags(String id) {
    return TAG_STORE.computeIfAbsent(id, key -> new CollaborationTagResponse(List.of(), null));
  }

  public CollaborationTagResponse saveTags(String id, CollaborationTagResponse tags) {
    CollaborationTagResponse updated =
        new CollaborationTagResponse(
            tags == null || tags.getTags() == null ? List.of() : tags.getTags(),
            "v" + System.currentTimeMillis());
    TAG_STORE.put(id, updated);
    return updated;
  }

  public List<Map<String, Object>> getUserStats(long start, long end) {
    return buildBasicDailySeries().stream()
        .map(row -> Map.<String, Object>of("date", row.date(), "total", row.uniqueUsers()))
        .toList();
  }

  public List<Map<String, Object>> getJobStats(long start, long end) {
    return buildBasicDailySeries().stream()
        .map(row -> Map.<String, Object>of("date", row.date(), "total", row.jobCount()))
        .toList();
  }

  public JobAndUserStatsResponse getJobsAndUsers(int numDaysBack, boolean detailedStats) {
    List<DailyRow> rows = buildSeries(numDaysBack);
    JobAndUserStatsResponse response = new JobAndUserStatsResponse();
    response.setEdition("dremio-oss-datafabric-0.0.1");
    if (!detailedStats) {
      response.setStats(
          rows.stream()
              .map(row -> new JobAndUserStatsResponse.Stat(row.date(), row.jobCount(), row.uniqueUsers()))
              .toList());
      return response;
    }

    response.setJobStats(
        rows.stream()
            .map(row -> new JobAndUserStatsResponse.JobStat(row.date(), row.jobCount(), row.uiRun(), row.rest()))
            .toList());
    List<JobAndUserStatsResponse.UserStat> userStats =
        rows.stream()
            .map(row -> new JobAndUserStatsResponse.UserStat(row.date(), row.uniqueUsers(), row.uiUsers(), row.restUsers()))
            .toList();
    response.setUserStatsByDate(userStats);
    response.setUserStatsByWeek(userStats.stream().limit(Math.min(2, userStats.size())).toList());
    response.setUserStatsByMonth(userStats.stream().limit(1).toList());
    return response;
  }

  private List<DailyRow> buildBasicDailySeries() {
    return buildSeries(7);
  }

  private CollaborationWikiResponse defaultWiki() {
    return new CollaborationWikiResponse("", null);
  }

  private SettingResponse supportFlagResponse(String key) {
    Object value =
        switch (key) {
          case "client.tools.tableau", "client.tools.powerbi", "ui.autocomplete.allow", "ui.formatter.allow" -> Boolean.FALSE;
          case "ui.upload.allow", "sqlrunner.tabs_ui", "ui.download.allow" -> Boolean.TRUE;
          default -> Boolean.FALSE;
        };
    return new SettingResponse(key, "BOOLEAN", value);
  }

  private Set<String> defaultSupportFlagKeys() {
    return Set.of(
        "client.tools.tableau",
        "client.tools.powerbi",
        "ui.autocomplete.allow",
        "ui.formatter.allow",
        "ui.upload.allow",
        "sqlrunner.tabs_ui",
        "ui.download.allow");
  }

  private CatalogItemResponse homeCatalogItem() {
    CatalogItemResponse item = new CatalogItemResponse();
    item.setId("11111111-1111-1111-1111-111111111111");
    item.setPath(List.of("@datafabric"));
    item.setType("CONTAINER");
    item.setContainerType("HOME");
    item.setTag("datafabric-home-tag");
    item.setCreatedAt(Instant.now().minusSeconds(86400).toEpochMilli());
    return item;
  }

  private CatalogItemResponse samplesSourceCatalogItem() {
    CatalogItemResponse item = new CatalogItemResponse();
    item.setId(UUID.nameUUIDFromBytes("Samples".getBytes()).toString());
    item.setPath(List.of("Samples"));
    item.setType("CONTAINER");
    item.setContainerType("SOURCE");
    item.setTag("datafabric-source-tag");
    item.setCreatedAt(Instant.now().minusSeconds(7200).toEpochMilli());
    item.setSourceChangeState("NONE");
    return item;
  }

  private CatalogItemResponse spaceCatalogItem(SpaceRecord space) {
    CatalogItemResponse item = new CatalogItemResponse();
    item.setId(space.id());
    item.setPath(List.of(space.name()));
    item.setType("CONTAINER");
    item.setContainerType("SPACE");
    item.setTag(space.tag());
    item.setCreatedAt(space.createdAt().toEpochMilli());
    return item;
  }

  private CatalogItemResponse sourceCatalogItem(SourceRecord source) {
    CatalogItemResponse item = new CatalogItemResponse();
    item.setId(source.id());
    item.setPath(List.of(source.name()));
    item.setType("CONTAINER");
    item.setContainerType("SOURCE");
    item.setTag(source.tag());
    item.setCreatedAt(source.createdAt().toEpochMilli());
    item.setSourceChangeState("NONE");
    return item;
  }

  private CatalogItemResponse salesFactCatalogItem() {
    CatalogItemResponse item = new CatalogItemResponse();
    item.setId("22222222-2222-2222-2222-222222222222");
    item.setPath(List.of("Samples", "SALES_FACT"));
    item.setType("DATASET");
    item.setDatasetType("PROMOTED");
    item.setTag("sales-fact-tag");
    item.setCreatedAt(Instant.now().minusSeconds(3600).toEpochMilli());
    return item;
  }

  private Map<String, Object> homeEntity(Integer maxChildren) {
    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("entityType", "home");
    entity.put("id", homeCatalogItem().getId());
    entity.put("name", "@datafabric");
    entity.put("children", List.of());
    entity.put("nextPageToken", null);
    return entity;
  }

  private Map<String, Object> spaceEntity(SpaceRecord space, Integer maxChildren) {
    Map<String, Object> entity = new LinkedHashMap<>();
    // 这个结构同时服务 v3 catalog 和旧版 apiv2/space 详情页，字段尽量向前端常用形态靠齐。
    entity.put("entityType", "space");
    entity.put("id", space.id());
    entity.put("name", space.name());
    entity.put("description", space.description());
    entity.put("tag", space.tag());
    entity.put("containerType", "SPACE");
    entity.put("type", "CONTAINER");
    entity.put("path", List.of(space.name()));
    entity.put("fullPathList", List.of(space.name()));
    entity.put("resourcePath", "/space/" + space.name());
    entity.put("urlPath", "/space/" + space.name());
    entity.put("createdAt", space.createdAt().toEpochMilli());
    entity.put("ctime", space.createdAt().toEpochMilli());
    entity.put("contents", emptyContents());
    entity.put("links", Map.of("self", "/space/" + space.name(), "jobs", "/jobs", "query", "/new_query"));
    entity.put("children", List.of());
    entity.put("nextPageToken", null);
    entity.put("stats", Map.of("datasetCount", 0, "datasetCountBounded", false));
    return entity;
  }

  private Map<String, Object> samplesSourceEntity(Integer maxChildren) {
    int childrenLimit = maxChildren == null ? 100 : Math.max(0, maxChildren);
    List<CatalogItemResponse> children =
        childrenLimit == 0 ? List.of() : List.of(salesFactCatalogItem());
    Map<String, Object> metadataPolicy =
        Map.of(
            "authTTLMs", 86400000,
            "namesRefreshMs", 3600000,
            "datasetRefreshAfterMs", 3600000,
            "datasetExpireAfterMs", 10800000,
            "datasetUpdateMode", "PREFETCH_QUERIED",
            "deleteUnavailableDatasets", true,
            "autoPromoteDatasets", true);

    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("entityType", "source");
    entity.put("id", samplesSourceCatalogItem().getId());
    entity.put("name", "Samples");
    entity.put("tag", "datafabric-source-tag");
    entity.put("type", "H2");
    entity.put("createdAt", Instant.now().minusSeconds(7200).toEpochMilli());
    entity.put("state", Map.of("status", "good", "suggestedUserAction", "", "messages", List.of()));
    entity.put("config", Map.of("engine", "H2", "rootPath", "/", "allowCrossSourceSelection", false));
    entity.put("metadataPolicy", metadataPolicy);
    entity.put("accelerationActivePolicyType", "PERIOD");
    entity.put("accelerationGracePeriodMs", 3600000);
    entity.put("accelerationRefreshPeriodMs", 3600000);
    entity.put("accelerationRefreshSchedule", "0 0 8 * * *");
    entity.put("accelerationNeverExpire", false);
    entity.put("accelerationNeverRefresh", false);
    entity.put("allowCrossSourceSelection", false);
    entity.put("disableMetadataValidityCheck", false);
    entity.put("sourceChangeState", "NONE");
    entity.put("children", children);
    entity.put("nextPageToken", null);
    return entity;
  }

  private Map<String, Object> sourceCatalogEntity(SourceRecord source, Integer maxChildren) {
    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("entityType", "source");
    entity.put("id", source.id());
    entity.put("name", source.name());
    entity.put("tag", source.tag());
    entity.put("type", source.type());
    entity.put("createdAt", source.createdAt().toEpochMilli());
    entity.put("description", source.description());
    entity.put("state", Map.of("status", "good", "suggestedUserAction", "", "messages", List.of()));
    entity.put("config", source.config().isEmpty() ? Map.of("engine", source.type()) : source.config());
    entity.put("metadataPolicy", source.metadataPolicy().isEmpty() ? defaultMetadataPolicy() : source.metadataPolicy());
    entity.put("accelerationActivePolicyType", "PERIOD");
    entity.put("accelerationGracePeriodMs", 3600000);
    entity.put("accelerationRefreshPeriodMs", 3600000);
    entity.put("accelerationRefreshSchedule", "0 0 8 * * *");
    entity.put("accelerationNeverExpire", false);
    entity.put("accelerationNeverRefresh", false);
    entity.put("allowCrossSourceSelection", false);
    entity.put("disableMetadataValidityCheck", false);
    entity.put("sourceChangeState", "NONE");
    entity.put("children", List.of());
    entity.put("nextPageToken", null);
    return entity;
  }

  private Map<String, Object> sourceEntity(SourceRecord sourceRecord) {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("id", sourceRecord.id());
    source.put("name", sourceRecord.name());
    source.put("fullPathList", List.of(sourceRecord.name()));
    source.put("resourcePath", "/source/" + sourceRecord.name());
    source.put("type", sourceRecord.type());
    source.put("tag", sourceRecord.tag());
    source.put("description", sourceRecord.description());
    source.put("ctime", sourceRecord.createdAt().toEpochMilli());
    source.put("numberOfDatasets", 0);
    source.put("datasetCountBounded", false);
    source.put(
        "links",
        Map.of(
            "self", "/source/" + sourceRecord.name(),
            "rename", "/source/" + sourceRecord.name() + "/rename",
            "jobs", "/jobs",
            "format", "/source/" + sourceRecord.name() + "/folder_format",
            "file_preview", "/source/" + sourceRecord.name() + "/file_preview",
            "file_format", "/source/" + sourceRecord.name() + "/file_format"));
    source.put("state", Map.of("status", "good", "suggestedUserAction", "", "messages", List.of()));
    source.put("config", sourceRecord.config().isEmpty() ? Map.of("engine", sourceRecord.type()) : sourceRecord.config());
    source.put("metadataPolicy", sourceRecord.metadataPolicy().isEmpty() ? defaultMetadataPolicy() : sourceRecord.metadataPolicy());
    source.put("accelerationRefreshPeriod", 3600000);
    source.put("accelerationGracePeriod", 3600000);
    source.put("accelerationRefreshSchedule", "0 0 8 * * *");
    source.put("accelerationActivePolicyType", "PERIOD");
    source.put("accelerationNeverExpire", false);
    source.put("accelerationNeverRefresh", false);
    source.put("allowCrossSourceSelection", false);
    source.put("disableMetadataValidityCheck", false);
    source.put("sourceChangeState", "NONE");
    source.put("contents", folderContents("source", sourceRecord.name(), null));
    return source;
  }

  private Map<String, Object> emptyContents() {
    return Map.of(
        "datasets", List.of(),
        "files", List.of(),
        "folders", List.of(),
        "physicalDatasets", List.of(),
        "functions", List.of(),
        "canTagsBeSkipped", false,
        "isFileSystemSource", false,
        "isImpersonationEnabled", false);
  }

  private Map<String, Object> folderContents(String rootType, String rootName, String path) {
    if ("source".equals(rootType) && clickHouseSourceService.isClickHouseSource(rootName)) {
      return clickHouseContents(rootName, path);
    }
    String normalizedPath = normalizePath(path);
    List<Map<String, Object>> folders =
        FOLDERS.values().stream()
            .filter(folder -> folder.rootType().equals(rootType))
            .filter(folder -> folder.rootName().equalsIgnoreCase(rootName))
            .filter(folder -> Objects.equals(parentPath(folder.relativePath()), normalizedPath))
            .sorted((left, right) -> left.name().compareToIgnoreCase(right.name()))
            .map(folder -> folderEntity(folder, false))
            .toList();
    return Map.of(
        "datasets", List.of(),
        "files", List.of(),
        "folders", folders,
        "physicalDatasets", List.of(),
        "functions", List.of(),
        "canTagsBeSkipped", false,
        "isFileSystemSource", "source".equals(rootType),
        "isImpersonationEnabled", false);
  }

  private Map<String, Object> clickHouseContents(String sourceName, String path) {
    try {
      String normalizedPath = normalizePath(path);
      if (normalizedPath.isBlank()) {
        List<Map<String, Object>> folders =
            clickHouseSourceService.listDatabases(sourceName).stream()
                .map(database -> clickHouseDatabaseFolder(sourceName, database, false))
                .toList();
        return Map.of(
            "datasets", List.of(),
            "files", List.of(),
            "folders", folders,
            "physicalDatasets", List.of(),
            "functions", List.of(),
            "canTagsBeSkipped", false,
            "isFileSystemSource", false,
            "isImpersonationEnabled", false);
      }

      List<Map<String, Object>> datasets =
          clickHouseSourceService.listTables(sourceName, normalizedPath).stream()
              .map(table -> clickHousePhysicalDataset(sourceName, normalizedPath, table))
              .toList();
      return Map.of(
          "datasets", List.of(),
          "files", List.of(),
          "folders", List.of(),
          "physicalDatasets", datasets,
          "functions", List.of(),
          "canTagsBeSkipped", false,
          "isFileSystemSource", false,
          "isImpersonationEnabled", false);
    } catch (Exception ex) {
      return emptyContents();
    }
  }

  private Map<String, Object> clickHouseFolderEntity(
      String sourceName, String databaseName, boolean includeContents) {
    Map<String, Object> entity = clickHouseDatabaseFolder(sourceName, normalizePath(databaseName), includeContents);
    if (includeContents) {
      entity.put("contents", clickHouseContents(sourceName, databaseName));
    }
    return entity;
  }

  private Map<String, Object> clickHouseDatabaseFolder(
      String sourceName, String databaseName, boolean includeContents) {
    String normalizedDatabase = normalizePath(databaseName);
    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("id", sourceName + ":" + normalizedDatabase);
    entity.put("name", normalizedDatabase);
    entity.put("fileSystemFolder", false);
    entity.put("file", false);
    entity.put("queryable", false);
    entity.put("isPhysicalDataset", false);
    entity.put("fullPathList", List.of(sourceName, normalizedDatabase));
    entity.put("resourcePath", "/source/" + sourceName + "/folder/" + normalizedDatabase);
    entity.put("urlPath", "/source/" + sourceName + "/folder/" + normalizedDatabase);
    entity.put("version", "ch-folder-" + normalizedDatabase);
    entity.put("tag", "ch-folder-" + normalizedDatabase);
    entity.put("jobCount", 0);
    entity.put(
        "links",
        Map.of(
            "self", "/source/" + sourceName + "/folder/" + normalizedDatabase,
            "format", "/source/" + sourceName + "/folder_format/" + normalizedDatabase,
            "delete_format", "/source/" + sourceName + "/folder_format/" + normalizedDatabase,
            "rename", "/source/" + sourceName + "/folder/" + normalizedDatabase));
    entity.put(
        "folderFormat",
        fileFormatEntity(fileFormatRecord("source", sourceName, normalizedDatabase, true)));
    if (includeContents) {
      entity.put("contents", clickHouseContents(sourceName, normalizedDatabase));
    }
    return entity;
  }

  private Map<String, Object> clickHousePhysicalDataset(
      String sourceName, String databaseName, String tableName) {
    String id = sourceName + "." + databaseName + "." + tableName;
    List<String> fullPath = List.of(sourceName, databaseName, tableName);
    Map<String, Object> datasetConfig = new LinkedHashMap<>();
    datasetConfig.put("id", id);
    datasetConfig.put("name", tableName);
    datasetConfig.put("type", "PHYSICAL_DATASET");
    datasetConfig.put("fullPathList", fullPath);

    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("id", id);
    entity.put("datasetName", tableName);
    entity.put("jobCount", 0);
    entity.put("descendants", 0);
    entity.put("datasetType", "PHYSICAL_DATASET");
    entity.put("entityType", "physicalDataset");
    entity.put("resourcePath", "/dataset/" + sourceName + "." + databaseName + "." + tableName);
    entity.put("fullPathList", fullPath);
    entity.put("fullPath", fullPath);
    entity.put(
        "links",
        Map.of(
            "self", "/source/" + sourceName + "/folder/" + databaseName,
            "query",
                "/new_query?context="
                    + sourceName
                    + "."
                    + databaseName
                    + "&sql=SELECT%20*%20FROM%20%22"
                    + sourceName
                    + "%22.%22"
                    + databaseName
                    + "%22.%22"
                    + tableName
                    + "%22%20LIMIT%2050",
            "edit",
                "/new_query?context="
                    + sourceName
                    + "."
                    + databaseName
                    + "&sql=SELECT%20*%20FROM%20%22"
                    + sourceName
                    + "%22.%22"
                    + databaseName
                    + "%22.%22"
                    + tableName
                    + "%22%20LIMIT%2050",
            "jobs", "/jobs"));
    entity.put("datasetConfig", datasetConfig);
    return entity;
  }

  private Map<String, Object> defaultMetadataPolicy() {
    return Map.of(
        "updateMode", "PREFETCH_QUERIED",
        "namesRefreshMillis", 3600000,
        "datasetDefinitionRefreshAfterMillis", 3600000,
        "datasetDefinitionExpireAfterMillis", 10800000,
        "authTTLMillis", 86400000,
        "autoPromoteDatasets", true);
  }

  private SourceListResponse.SourceInfo toSourceInfo(SourceRecord record, boolean includeDatasetCount) {
    SourceListResponse.SourceInfo source = new SourceListResponse.SourceInfo();
    source.setType(record.type());
    source.setName(record.name());
    source.setCtime(record.createdAt().toEpochMilli());
    source.setId(record.id());
    source.setTag(record.tag());
    source.setResourcePath("/source/" + record.name());
    source.setFullPathList(List.of(record.name()));
    source.setLinks(Map.of("self", "/source/" + record.name(), "jobs", "/jobs", "query", "/new_query"));
    source.setState(Map.of("status", "good", "suggestedUserAction", "", "messages", List.of()));
    source.setConfig(record.config().isEmpty() ? Map.of("engine", record.type()) : record.config());
    source.setMetadataPolicy(record.metadataPolicy().isEmpty() ? defaultMetadataPolicy() : record.metadataPolicy());
    if (includeDatasetCount) {
      source.setNumberOfDatasets("Samples".equalsIgnoreCase(record.name()) ? 1 : 0);
    }
    return source;
  }

  private SourceRecord defaultSampleSource() {
    return new SourceRecord(
        UUID.nameUUIDFromBytes("Samples".getBytes()).toString(),
        "Samples",
        "H2",
        "Datafabric sample source",
        "datafabric-source-tag",
        Instant.now().minusSeconds(7200),
        Map.of("engine", "H2", "rootPath", "/", "allowCrossSourceSelection", false),
        defaultMetadataPolicy());
  }

  private SourceRecord findSourceByName(String sourceName) {
    if ("Samples".equalsIgnoreCase(sourceName)) {
      return defaultSampleSource();
    }
    SourceRecord source = findSourceByNameOrNull(sourceName);
    if (source == null) {
      throw new NoSuchElementException("Source not found: " + sourceName);
    }
    return source;
  }

  private SourceRecord findSourceByNameOrNull(String sourceName) {
    if (sourceName == null) {
      return null;
    }
    return SOURCES_BY_NAME.get(sourceName.toLowerCase());
  }

  private void ensureSourceNameAvailable(String name, String currentId) {
    if ("Samples".equalsIgnoreCase(name) && currentId == null) {
      throw new IllegalArgumentException("A source with this name already exists");
    }
    boolean exists =
        SOURCES.values().stream()
            .anyMatch(source -> !Objects.equals(source.id(), currentId) && source.name().equalsIgnoreCase(name));
    if (exists) {
      throw new IllegalArgumentException("A source with this name already exists");
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> extractMap(Object value) {
    if (value instanceof Map<?, ?> map) {
      return new LinkedHashMap<>((Map<String, Object>) map);
    }
    return Map.of();
  }

  private Map<String, Object> salesFactDatasetEntity() {
    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("entityType", "dataset");
    entity.put("id", salesFactCatalogItem().getId());
    entity.put("path", List.of("Samples", "SALES_FACT"));
    entity.put("type", "PHYSICAL_DATASET");
    entity.put("createdAt", Instant.now().minusSeconds(3600).toString());
    entity.put(
        "fields",
        List.of(
            Map.of("name", "id", "type", Map.of("name", "INTEGER"), "isPartitioned", false, "isSorted", false),
            Map.of("name", "amount", "type", Map.of("name", "DOUBLE"), "isPartitioned", false, "isSorted", false)));
    entity.put("schemaOutdated", false);
    return entity;
  }

  private SpaceRecord requireSpace(String id) {
    SpaceRecord space = SPACES.get(id);
    if (space == null) {
      throw new NoSuchElementException("Catalog entity not found: " + id);
    }
    return space;
  }

  private void ensureSpaceNameAvailable(String name, String currentId) {
    // 创建和重命名都走同一套名字冲突校验，避免前端刷新列表时出现重复节点。
    boolean exists =
        SPACES.values().stream()
            .anyMatch(space -> !Objects.equals(space.id(), currentId) && space.name().equalsIgnoreCase(name));
    if (exists) {
      throw new IllegalArgumentException("A space with this name already exists");
    }
  }

  private String normalizeString(Object value) {
    return value == null ? "" : value.toString().trim();
  }

  private Map<String, Object> missingCatalogPath(List<String> path) {
    throw new NoSuchElementException("Catalog path not found: " + path);
  }

  private Map<String, Object> folderEntity(FolderRecord folder, boolean includeContents) {
    List<String> fullPathList = new ArrayList<>();
    fullPathList.add(folder.rootName());
    if (!folder.relativePath().isBlank()) {
      fullPathList.addAll(List.of(folder.relativePath().split("/")));
    }
    Map<String, Object> entity = new LinkedHashMap<>();
    entity.put("id", folder.id());
    entity.put("name", folder.name());
    entity.put("fileSystemFolder", "source".equals(folder.rootType()));
    entity.put("file", false);
    entity.put("queryable", false);
    entity.put("isPhysicalDataset", false);
    entity.put("fullPathList", fullPathList);
    entity.put("resourcePath", folderSelfPath(folder.rootType(), folder.rootName(), folder.relativePath()));
    entity.put("urlPath", folderSelfPath(folder.rootType(), folder.rootName(), folder.relativePath()));
    entity.put("version", folder.tag());
    entity.put("tag", folder.tag());
    entity.put("jobCount", 0);
    entity.put("storageUri", folder.storageUri());
    entity.put(
        "links",
        Map.of(
            "self", folderSelfPath(folder.rootType(), folder.rootName(), folder.relativePath()),
            "format", formatPath(folder.rootType(), folder.rootName(), folder.relativePath(), true),
            "delete_format", formatPath(folder.rootType(), folder.rootName(), folder.relativePath(), true),
            "rename", folderSelfPath(folder.rootType(), folder.rootName(), folder.relativePath())));
    entity.put(
        "folderFormat",
        fileFormatEntity(fileFormatRecord(folder.rootType(), folder.rootName(), folder.relativePath(), true)));
    if (includeContents) {
      entity.put("contents", folderContents(folder.rootType(), folder.rootName(), folder.relativePath()));
    }
    return entity;
  }

  private FileFormatRecord fileFormatRecord(
      String rootType, String rootName, String path, boolean folderFormat) {
    String key = fileFormatKey(rootType, rootName, path, folderFormat);
    return FILE_FORMATS.computeIfAbsent(
        key,
        ignored ->
            new FileFormatRecord(
                UUID.randomUUID().toString(),
                rootType,
                rootName,
                normalizePath(path),
                folderFormat,
                "JSON",
                "format-" + Instant.now().toEpochMilli(),
                Map.of()));
  }

  private Map<String, Object> fileFormatEntity(FileFormatRecord format) {
    List<String> fullPath = new ArrayList<>();
    fullPath.add(format.rootName());
    if (!format.relativePath().isBlank()) {
      fullPath.addAll(List.of(format.relativePath().split("/")));
    }
    Map<String, Object> entity = new LinkedHashMap<>(format.payload());
    entity.put("id", format.id());
    entity.put("version", format.version());
    entity.put("type", format.type());
    entity.put("fullPath", fullPath);
    entity.put("location", String.join("/", fullPath));
    entity.put(
        "links",
        Map.of("self", formatPath(format.rootType(), format.rootName(), format.relativePath(), format.folderFormat())));
    return entity;
  }

  private FolderRecord requireFolder(String rootType, String rootName, String path) {
    validateRoot(rootType, rootName);
    FolderRecord folder = FOLDERS.get(folderKey(rootType, rootName, path));
    if (folder == null) {
      throw new NoSuchElementException("Folder not found: " + normalizePath(path));
    }
    return folder;
  }

  private void validateRoot(String rootType, String rootName) {
    switch (rootType) {
      case "source" -> findSourceByName(rootName);
      case "space" ->
          SPACES.values().stream()
              .filter(item -> item.name().equalsIgnoreCase(rootName))
              .findFirst()
              .orElseThrow(() -> new NoSuchElementException("Space not found: " + rootName));
      case "home" -> getHome(rootName, false);
      default -> throw new IllegalArgumentException("Unsupported root type: " + rootType);
    }
  }

  private void renameFolderTree(
      FolderRecord existing, String nextRelativePath, String nextName, String nextStorageUri) {
    String oldPrefix = existing.relativePath();
    List<FolderRecord> affected =
        FOLDERS.values().stream()
            .filter(folder -> folder.rootType().equals(existing.rootType()))
            .filter(folder -> folder.rootName().equalsIgnoreCase(existing.rootName()))
            .filter(
                folder ->
                    folder.relativePath().equals(oldPrefix)
                        || folder.relativePath().startsWith(oldPrefix + "/"))
            .toList();
    for (FolderRecord folder : affected) {
      FOLDERS.remove(folderKey(folder.rootType(), folder.rootName(), folder.relativePath()));
    }
    for (FolderRecord folder : affected) {
      String suffix =
          folder.relativePath().equals(oldPrefix)
              ? ""
              : folder.relativePath().substring(oldPrefix.length());
      String rewritten = nextRelativePath + suffix;
      String rewrittenName = folder.relativePath().equals(oldPrefix) ? nextName : leafName(rewritten);
      String storageUri = folder.relativePath().equals(oldPrefix) ? nextStorageUri : folder.storageUri();
      FOLDERS.put(
          folderKey(folder.rootType(), folder.rootName(), rewritten),
          new FolderRecord(
              folder.id(),
              folder.rootType(),
              folder.rootName(),
              rewritten,
              rewrittenName,
              "folder-" + Instant.now().toEpochMilli(),
              folder.createdAt(),
              storageUri));
    }
  }

  private String folderKey(String rootType, String rootName, String path) {
    return rootType + "|" + rootName.toLowerCase() + "|" + normalizePath(path);
  }

  private String fileFormatKey(String rootType, String rootName, String path, boolean folderFormat) {
    return rootType
        + "|"
        + rootName.toLowerCase()
        + "|"
        + (folderFormat ? "folder" : "file")
        + "|"
        + normalizePath(path);
  }

  private String normalizePath(String path) {
    if (path == null) {
      return "";
    }
    String normalized = path.trim();
    while (normalized.startsWith("/")) {
      normalized = normalized.substring(1);
    }
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }

  private String parentPath(String path) {
    String normalized = normalizePath(path);
    int index = normalized.lastIndexOf('/');
    return index < 0 ? "" : normalized.substring(0, index);
  }

  private String leafName(String path) {
    String normalized = normalizePath(path);
    int index = normalized.lastIndexOf('/');
    return index < 0 ? normalized : normalized.substring(index + 1);
  }

  private String folderSelfPath(String rootType, String rootName, String path) {
    String normalized = normalizePath(path);
    return normalized.isBlank()
        ? "/" + rootType + "/" + rootName + "/folder/"
        : "/" + rootType + "/" + rootName + "/folder/" + normalized;
  }

  private String formatPath(String rootType, String rootName, String path, boolean folderFormat) {
    String normalized = normalizePath(path);
    String prefix = folderFormat ? "folder_format" : "file_format";
    return normalized.isBlank()
        ? "/" + rootType + "/" + rootName + "/" + prefix
        : "/" + rootType + "/" + rootName + "/" + prefix + "/" + normalized;
  }

  private Map<String, Object> mergeMaps(Map<String, Object> existing, Map<String, Object> updates) {
    Map<String, Object> merged = new LinkedHashMap<>(existing);
    merged.putAll(updates);
    return merged;
  }

  private record SpaceRecord(String id, String name, String description, String tag, Instant createdAt) {}

  private record SourceRecord(
      String id,
      String name,
      String type,
      String description,
      String tag,
      Instant createdAt,
      Map<String, Object> config,
      Map<String, Object> metadataPolicy) {}

  private record FolderRecord(
      String id,
      String rootType,
      String rootName,
      String relativePath,
      String name,
      String tag,
      Instant createdAt,
      String storageUri) {}

  private record FileFormatRecord(
      String id,
      String rootType,
      String rootName,
      String relativePath,
      boolean folderFormat,
      String type,
      String version,
      Map<String, Object> payload) {}

  private void savePersistedSource(SourceRecord source) {
    try {
      Files.createDirectories(sourceStoreDir());
      objectMapper.writeValue(sourceStoreDir().resolve(source.name().toLowerCase() + ".json").toFile(), source);
    } catch (IOException ex) {
      throw new IllegalStateException("Failed to persist source: " + source.name(), ex);
    }
  }

  private Path sourceStoreDir() {
    return properties.getBaseDir().resolve("sources");
  }

  private void deletePersistedSource(String sourceName) {
    try {
      Files.deleteIfExists(sourceStoreDir().resolve(sourceName.toLowerCase() + ".json"));
    } catch (IOException ex) {
      throw new IllegalStateException("Failed to delete persisted source: " + sourceName, ex);
    }
  }

  private List<Path> listSourceFiles() throws IOException {
    try (var stream = Files.list(sourceStoreDir())) {
      return stream.filter(path -> path.getFileName().toString().endsWith(".json")).sorted().toList();
    }
  }

  private SourceTypeTemplateResponse sourceType(
      String sourceType,
      String label,
      String icon,
      boolean externalQueryAllowed,
      boolean previewEngineRequired) {
    SourceTypeTemplateResponse response = new SourceTypeTemplateResponse();
    response.setSourceType(sourceType);
    response.setLabel(label);
    response.setIcon(icon);
    response.setExternalQueryAllowed(externalQueryAllowed);
    response.setPreviewEngineRequired(previewEngineRequired);
    response.setElements(defaultSourceElements(sourceType));
    response.setUiConfig(defaultSourceUiConfig(sourceType, label));
    return response;
  }

  private SourceTypeTemplateResponse withUiConfig(SourceTypeTemplateResponse response) {
    response.setUiConfig(
        defaultSourceUiConfig(response.getSourceType(), response.getLabel()));
    return response;
  }

  private boolean isFileSystemSource(String sourceType) {
    return List.of("S3", "HDFS", "NAS").contains(sourceType.toUpperCase());
  }

  private List<Map<String, Object>> defaultSourceElements(String sourceType) {
    return switch (sourceType.toUpperCase()) {
      case "HIVE", "HIVE3" ->
          List.of(
              element("text", "hostname", "Hive Metastore Host", null),
              element("number", "port", "Port", 9083),
              element("checkbox", "enableSasl", "Enable SASL", true),
              element("text", "kerberosPrincipal", "Hive Kerberos Principal", null),
              element("property_list", "propertyList", "Properties", null));
      case "HDFS" ->
          List.of(
              element("text", "hostname", "NameNode Host", null),
              element("number", "port", "Port", 8020),
              element("checkbox", "enableImpersonation", "Enable impersonation", false),
              element("text", "rootPath", "Root Path", "/"),
              element("property_list", "propertyList", "Properties", null));
      case "NAS" ->
          List.of(element("text", "path", "Root Path", "/"));
      case "S3" ->
          List.of(
              element("text", "accessKey", "AWS Access Key", null),
              secretElement("accessSecret", "AWS Access Secret"),
              element("checkbox", "secure", "Encrypt connection", true),
              element("value_list", "externalBucketList[]", "Bucket", null),
              element("property_list", "propertyList", "Properties", null));
      case "MYSQL" ->
          List.of(
              element("text", "hostname", "Host", null),
              element("number", "port", "Port", 3306),
              element("text", "username", "Username", null),
              secretElement("password", "Password"),
              element("checkbox", "useSsl", "Encrypt connection", false));
      case "POSTGRES" ->
          List.of(
              element("text", "hostname", "Host", null),
              element("number", "port", "Port", 5432),
              element("text", "databaseName", "Database", null),
              element("text", "username", "Username", null),
              secretElement("password", "Password"),
              element("checkbox", "useSsl", "Encrypt connection", false));
      case "CLICKHOUSE" ->
          List.of(
              element("text", "hostname", "Host", null),
              element("number", "port", "Port", 8123),
              element("text", "username", "Username", null),
              secretElement("password", "Password"),
              element("checkbox", "tls", "Encrypt connection", false),
              element("text", "rootPath", "Default Schema", null));
      default -> List.of();
    };
  }

  private Map<String, Object> defaultSourceUiConfig(String sourceType, String label) {
    Map<String, Object> uiConfig = new LinkedHashMap<>();
    uiConfig.put("sourceType", sourceType);
    uiConfig.put("label", label);
    uiConfig.put(
        "metadataRefresh",
        Map.of(
            "isFileSystemSource", isFileSystemSource(sourceType),
            "datasetDiscovery", true,
            "authorization", false));

    Map<String, Object> generalTab = new LinkedHashMap<>();
    generalTab.put("name", "General");
    generalTab.put("isGeneral", true);
    generalTab.put("sections", List.of(section("Connection", uiElementsFor(sourceType))));

    Map<String, Object> form = new LinkedHashMap<>();
    form.put("tabs", List.of(generalTab));
    uiConfig.put("form", form);
    return uiConfig;
  }

  private List<Map<String, Object>> uiElementsFor(String sourceType) {
    return defaultSourceElements(sourceType).stream()
        .map(
            element -> {
              Map<String, Object> uiElement = new LinkedHashMap<>();
              uiElement.put("propName", element.get("propertyName"));
              uiElement.put("label", element.get("label"));
              uiElement.put("type", element.get("type"));
              if (element.containsKey("secret")) {
                uiElement.put("secure", element.get("secret"));
              }
              if (element.containsKey("value")) {
                uiElement.put("value", element.get("value"));
              }
              return uiElement;
            })
        .toList();
  }

  private Map<String, Object> section(String name, List<Map<String, Object>> elements) {
    Map<String, Object> section = new LinkedHashMap<>();
    section.put("name", name);
    section.put("elements", elements);
    return section;
  }

  private Map<String, Object> element(String type, String propertyName, String label, Object value) {
    Map<String, Object> element = new LinkedHashMap<>();
    element.put("type", type);
    element.put("propertyName", propertyName);
    if (label != null) {
      element.put("label", label);
    }
    if (value != null) {
      element.put("defaultValue", value);
      element.put("value", value);
    }
    return element;
  }

  private Map<String, Object> secretElement(String propertyName, String label) {
    Map<String, Object> element = element("text", propertyName, label, null);
    element.put("secret", true);
    return element;
  }

  private List<DailyRow> buildSeries(int numDaysBack) {
    int days = Math.max(1, Math.min(numDaysBack, 30));
    List<JobRecord> jobs = jobService.listJobs();
    List<DailyRow> rows = new ArrayList<>();
    for (int i = days - 1; i >= 0; i--) {
      LocalDate date = LocalDate.now().minusDays(i);
      List<JobRecord> sameDayJobs =
          jobs.stream()
              .filter(job -> job.getCreatedAt() != null)
              .filter(
                  job ->
                      job.getCreatedAt()
                          .atZone(ZoneId.systemDefault())
                          .toLocalDate()
                          .equals(date))
              .toList();
      long rest = sameDayJobs.size();
      long uiRun = 0L;
      long total = rest + uiRun;
      long uniqueUsers = total > 0 ? 1L : 0L;
      rows.add(new DailyRow(date.format(DATE_FORMATTER), total, uniqueUsers, uiRun, rest, 0L, uniqueUsers));
    }
    return rows;
  }

  private record DailyRow(
      String date, long jobCount, long uniqueUsers, long uiRun, long rest, long uiUsers, long restUsers) {}

  public Map<String, Object> listJobs(int offset, int limit, String filter, String sort, String order) {
    List<JobRecord> allJobs = jobService.listJobs();
    
    // Apply filter if provided
    List<Map<String, Object>> jobList = allJobs.stream()
        .map(job -> {
          Map<String, Object> item = new LinkedHashMap<>();
          item.put("id", job.getId());
          item.put("jobId", Map.of("id", job.getId()));
          item.put("state", job.getStatus().name());
          item.put("jobStatus", job.getStatus().name());
          item.put("description", truncateSql(job.getSql(), 200));
          item.put("sql", job.getSql());
          item.put("startTime", toEpoch(job.getCreatedAt()));
          item.put("endTime", toEpoch(job.getCompletedAt()));
          item.put("outputRecords", job.getRowCount() == null ? 0L : job.getRowCount());
          item.put("queryType", "UI_RUN");
          item.put("accelerated", false);
          return item;
        })
        .toList();

    // Apply pagination
    int fromIndex = Math.min(offset, jobList.size());
    int toIndex = Math.min(offset + limit, jobList.size());
    List<Map<String, Object>> pagedJobs = fromIndex < jobList.size() 
        ? jobList.subList(fromIndex, toIndex) 
        : List.of();

    return Map.of(
        "jobs", pagedJobs,
        "total", jobList.size(),
        "offset", offset,
        "limit", limit
    );
  }

  private long toEpoch(Instant instant) {
    return instant == null ? 0L : instant.toEpochMilli();
  }

  private String truncateSql(String sql, int maxLength) {
    if (sql == null || sql.isBlank()) {
      return "";
    }
    if (maxLength <= 0 || sql.length() <= maxLength) {
      return sql;
    }
    return sql.substring(0, maxLength) + "...";
  }
}
