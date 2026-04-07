package com.datafabric.service;

import com.datafabric.dto.UserPreferenceResponse;
import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;

@Service
public class UserPreferenceService {
  
  private static final String DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000001";
  
  // 偏好类型常量
  public static final String PREFERENCE_TYPE_HOMES = "HOMES";
  public static final String PREFERENCE_TYPE_SPACES = "SPACES";
  public static final String PREFERENCE_TYPE_SOURCES = "SOURCES";
  public static final String PREFERENCE_TYPE_WATCHED = "WATCHED";
  public static final String PREFERENCE_TYPE_FAVORITES = "FAVORITES";
  public static final String PREFERENCE_TYPE_SQL_EDITOR = "SQL_EDITOR";
  
  // 用户偏好存储: userId -> preferenceType -> List<Entity>
  private final ConcurrentMap<String, ConcurrentMap<String, List<PreferenceEntity>>> preferences = 
      new ConcurrentHashMap<>();
  
  @PostConstruct
  void init() {
    // 初始化默认用户的偏好设置
    ConcurrentMap<String, List<PreferenceEntity>> userPrefs = new ConcurrentHashMap<>();
    
    // 添加默认的 home 偏好
    List<PreferenceEntity> homeEntities = new ArrayList<>();
    homeEntities.add(new PreferenceEntity(
        "11111111-1111-1111-1111-111111111111",
        "@datafabric",
        List.of("@datafabric"),
        "HOME",
        Instant.now().toEpochMilli()
    ));
    userPrefs.put(PREFERENCE_TYPE_HOMES, homeEntities);
    
    preferences.put(DEFAULT_USER_ID, userPrefs);
  }
  
  public UserPreferenceResponse getPreference(String preferenceType, boolean showCatalogInfo) {
    validatePreferenceType(preferenceType);
    
    List<PreferenceEntity> entities = getUserPreferences(DEFAULT_USER_ID)
        .getOrDefault(preferenceType.toUpperCase(), new ArrayList<>());
    
    List<Map<String, Object>> entityList = entities.stream()
        .map(e -> {
          if (showCatalogInfo) {
            return Map.<String, Object>of(
                "id", e.entityId,
                "name", e.name,
                "fullPath", e.fullPath,
                "type", e.type,
                "timestamp", e.timestamp
            );
          } else {
            return Map.<String, Object>of(
                "id", e.entityId,
                "timestamp", e.timestamp
            );
          }
        })
        .toList();
    
    return new UserPreferenceResponse(preferenceType.toUpperCase(), entityList);
  }
  
  public UserPreferenceResponse addPreference(String preferenceType, String entityId) {
    validatePreferenceType(preferenceType);
    
    ConcurrentMap<String, List<PreferenceEntity>> userPrefs = 
        preferences.computeIfAbsent(DEFAULT_USER_ID, k -> new ConcurrentHashMap<>());
    
    List<PreferenceEntity> entities = 
        userPrefs.computeIfAbsent(preferenceType.toUpperCase(), k -> new ArrayList<>());
    
    // 检查是否已存在
    boolean exists = entities.stream()
        .anyMatch(e -> e.entityId.equals(entityId));
    
    if (!exists) {
      entities.add(new PreferenceEntity(
          entityId,
          "Entity-" + entityId.substring(0, Math.min(8, entityId.length())),
          List.of(entityId),
          preferenceTypeToEntityType(preferenceType),
          Instant.now().toEpochMilli()
      ));
    }
    
    return getPreference(preferenceType, false);
  }
  
  public UserPreferenceResponse removePreference(String preferenceType, String entityId) {
    validatePreferenceType(preferenceType);
    
    ConcurrentMap<String, List<PreferenceEntity>> userPrefs = preferences.get(DEFAULT_USER_ID);
    if (userPrefs != null) {
      List<PreferenceEntity> entities = userPrefs.get(preferenceType.toUpperCase());
      if (entities != null) {
        entities.removeIf(e -> e.entityId.equals(entityId));
      }
    }
    
    return getPreference(preferenceType, false);
  }
  
  private Map<String, List<PreferenceEntity>> getUserPreferences(String userId) {
    return preferences.getOrDefault(userId, new ConcurrentHashMap<>());
  }
  
  private void validatePreferenceType(String preferenceType) {
    if (preferenceType == null || preferenceType.isBlank()) {
      throw new IllegalArgumentException("Preference type is required");
    }
    String upperType = preferenceType.toUpperCase();
    if (!isValidPreferenceType(upperType)) {
      throw new IllegalArgumentException(
          String.format("%s is not a valid preference type. Valid types are: %s", 
              preferenceType, String.join(", ", getValidPreferenceTypes())));
    }
  }
  
  private boolean isValidPreferenceType(String type) {
    return PREFERENCE_TYPE_HOMES.equals(type) ||
           PREFERENCE_TYPE_SPACES.equals(type) ||
           PREFERENCE_TYPE_SOURCES.equals(type) ||
           PREFERENCE_TYPE_WATCHED.equals(type) ||
           PREFERENCE_TYPE_FAVORITES.equals(type) ||
           PREFERENCE_TYPE_SQL_EDITOR.equals(type);
  }
  
  private List<String> getValidPreferenceTypes() {
    return List.of(
        PREFERENCE_TYPE_HOMES,
        PREFERENCE_TYPE_SPACES,
        PREFERENCE_TYPE_SOURCES,
        PREFERENCE_TYPE_WATCHED,
        PREFERENCE_TYPE_FAVORITES,
        PREFERENCE_TYPE_SQL_EDITOR
    );
  }
  
  private String preferenceTypeToEntityType(String preferenceType) {
    return switch (preferenceType.toUpperCase()) {
      case PREFERENCE_TYPE_HOMES -> "HOME";
      case PREFERENCE_TYPE_SPACES -> "SPACE";
      case PREFERENCE_TYPE_SOURCES -> "SOURCE";
      case PREFERENCE_TYPE_FAVORITES, PREFERENCE_TYPE_WATCHED -> "DATASET";
      default -> "ENTITY";
    };
  }
  
  // 内部实体类
  private record PreferenceEntity(
      String entityId,
      String name,
      List<String> fullPath,
      String type,
      long timestamp
  ) {}
}
