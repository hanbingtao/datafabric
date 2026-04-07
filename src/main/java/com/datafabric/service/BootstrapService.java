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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;

@Service
public class BootstrapService {
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
  private final Map<String, CollaborationWikiResponse> wikiStore = new ConcurrentHashMap<>();
  private final Map<String, CollaborationTagResponse> tagStore = new ConcurrentHashMap<>();

  private final JobService jobService;
  private final MetadataService metadataService;

  public BootstrapService(JobService jobService, MetadataService metadataService) {
    this.jobService = jobService;
    this.metadataService = metadataService;
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
    SourceListResponse.SourceInfo source = new SourceListResponse.SourceInfo();
    source.setType("H2");
    source.setName("Samples");
    source.setCtime(Instant.now().minusSeconds(7200).toEpochMilli());
    source.setId(UUID.nameUUIDFromBytes("Samples".getBytes()).toString());
    source.setTag("datafabric-source-tag");
    source.setResourcePath("/source/Samples");
    source.setFullPathList(List.of("Samples"));
    source.setLinks(
        Map.of(
            "self", "/source/Samples",
            "jobs", "/jobs",
            "query", "/new_query"));
    source.setState(Map.of("status", "good", "suggestedUserAction", "", "messages", List.of()));
    source.setConfig(Map.of("engine", "H2", "rootPath", "/", "allowCrossSourceSelection", false));
    source.setMetadataPolicy(
        Map.of(
            "updateMode", "PREFETCH_QUERIED",
            "namesRefreshMillis", 3600000,
            "datasetDefinitionRefreshAfterMillis", 3600000,
            "datasetDefinitionExpireAfterMillis", 10800000,
            "authTTLMillis", 86400000,
            "autoPromoteDatasets", true));
    if (includeDatasetCount) {
      try {
        source.setNumberOfDatasets(metadataService.listDatasets().size());
      } catch (Exception ex) {
        source.setNumberOfDatasets(1);
      }
    }
    return new SourceListResponse(List.of(source));
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
      home.put(
          "contents",
          Map.of(
              "datasets", List.of(),
              "files", List.of(),
              "folders", List.of(),
              "physicalDatasets", List.of(),
              "functions", List.of(),
              "canTagsBeSkipped", false,
              "isFileSystemSource", false,
              "isImpersonationEnabled", false));
    }

    return home;
  }

  public Map<String, Object> getSource(String sourceName, boolean includeContents) {
    if (!"Samples".equalsIgnoreCase(sourceName)) {
      throw new NoSuchElementException("Source not found: " + sourceName);
    }

    Map<String, Object> source = new LinkedHashMap<>();
    source.put("id", samplesSourceCatalogItem().getId());
    source.put("name", "Samples");
    source.put("fullPathList", List.of("Samples"));
    source.put("resourcePath", "/source/Samples");
    source.put("type", "H2");
    source.put("tag", "datafabric-source-tag");
    source.put("description", "Datafabric sample source");
    source.put("ctime", Instant.now().minusSeconds(7200).toEpochMilli());
    source.put("numberOfDatasets", 1);
    source.put("datasetCountBounded", false);
    source.put(
        "links",
        Map.of(
            "self", "/source/Samples",
            "rename", "/source/Samples/rename",
            "jobs", "/jobs",
            "format", "/source/Samples/folder_format",
            "file_preview", "/source/Samples/file_preview",
            "file_format", "/source/Samples/file_format"));
    source.put(
        "state",
        Map.of(
            "status", "good",
            "suggestedUserAction", "",
            "messages", List.of()));
    source.put("config", Map.of("engine", "H2", "rootPath", "/", "allowCrossSourceSelection", false));
    source.put("metadataPolicy", samplesSourceEntity(1).get("metadataPolicy"));
    source.put("accelerationRefreshPeriod", 3600000);
    source.put("accelerationGracePeriod", 3600000);
    source.put("accelerationRefreshSchedule", "0 0 8 * * *");
    source.put("accelerationActivePolicyType", "PERIOD");
    source.put("accelerationNeverExpire", false);
    source.put("accelerationNeverRefresh", false);
    source.put("allowCrossSourceSelection", false);
    source.put("disableMetadataValidityCheck", false);
    source.put("sourceChangeState", "NONE");
    if (includeContents) {
      source.put(
          "contents",
          Map.of(
              "datasets", List.of(),
              "files", List.of(),
              "folders", List.of(),
              "physicalDatasets", List.of(),
              "functions", List.of(),
              "canTagsBeSkipped", false,
              "isFileSystemSource", false,
              "isImpersonationEnabled", false));
    }
    return source;
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
    return Map.of("functions", List.of());
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
    return List.of(homeCatalogItem(), samplesSourceCatalogItem());
  }

  public Map<String, Object> getCatalogById(String id, Integer maxChildren) {
    if (samplesSourceCatalogItem().getId().equals(id)) {
      return samplesSourceEntity(maxChildren);
    }
    if (homeCatalogItem().getId().equals(id)) {
      return homeEntity(maxChildren);
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
    if (path.size() == 1 && "@datafabric".equalsIgnoreCase(path.get(0))) {
      return homeEntity(maxChildren);
    }
    if (path.size() == 2
        && "Samples".equalsIgnoreCase(path.get(0))
        && "SALES_FACT".equalsIgnoreCase(path.get(1))) {
      return salesFactDatasetEntity();
    }
    throw new NoSuchElementException("Catalog path not found: " + path);
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
    return new UserPreferenceResponse(preferenceType, List.of());
  }

  public UserPreferenceResponse addPreference(String preferenceType, String entityId) {
    return new UserPreferenceResponse(preferenceType, List.of(Map.of("id", entityId)));
  }

  public UserPreferenceResponse removePreference(String preferenceType, String entityId) {
    return new UserPreferenceResponse(preferenceType, List.of());
  }

  public CollaborationWikiResponse getWiki(String id) {
    return wikiStore.computeIfAbsent(id, key -> defaultWiki());
  }

  public CollaborationWikiResponse saveWiki(String id, CollaborationWikiResponse wiki) {
    long nextVersion = getWiki(id).getVersion() == null ? 0L : getWiki(id).getVersion() + 1L;
    CollaborationWikiResponse updated =
        new CollaborationWikiResponse(wiki == null ? "" : wiki.getText(), nextVersion);
    wikiStore.put(id, updated);
    return updated;
  }

  public CollaborationTagResponse getTags(String id) {
    return tagStore.computeIfAbsent(id, key -> new CollaborationTagResponse(List.of(), null));
  }

  public CollaborationTagResponse saveTags(String id, CollaborationTagResponse tags) {
    CollaborationTagResponse updated =
        new CollaborationTagResponse(
            tags == null || tags.getTags() == null ? List.of() : tags.getTags(),
            "v" + System.currentTimeMillis());
    tagStore.put(id, updated);
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
}
