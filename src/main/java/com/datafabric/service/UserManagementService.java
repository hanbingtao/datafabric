package com.datafabric.service;

import com.datafabric.dto.ApiUserResponse;
import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Service;

@Service
public class UserManagementService {
  private static final String DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000001";
  private static final String ADMIN_ROLE = "admin";
  private static final String USER_ROLE = "user";

  private final ConcurrentMap<String, UserRecord> users = new ConcurrentHashMap<>();

  @PostConstruct
  void init() {
    // 创建默认用户
    UserRecord defaultUser = new UserRecord();
    defaultUser.setId(DEFAULT_USER_ID);
    defaultUser.setUserName("datafabric");
    defaultUser.setFirstName("Data");
    defaultUser.setLastName("Fabric");
    defaultUser.setEmail("datafabric@local");
    defaultUser.setCreatedAt(Instant.now().minusSeconds(86400 * 7));
    defaultUser.setModifiedAt(Instant.now().minusSeconds(86400 * 7));
    defaultUser.setActive(true);
    defaultUser.setTag("default-user-tag");
    defaultUser.setRoles(List.of(ADMIN_ROLE, USER_ROLE));
    defaultUser.setDescription("Default datafabric user");
    users.put(defaultUser.getUserName().toLowerCase(), defaultUser);
  }

  public ApiUserResponse getUserByName(String userName) {
    UserRecord user = users.get(userName.toLowerCase());
    if (user == null) {
      throw new NoSuchElementException("User not found: " + userName);
    }
    return toApiUserResponse(user);
  }

  public ApiUserResponse getUserById(String userId) {
    return users.values().stream()
        .filter(u -> u.getId().equals(userId))
        .findFirst()
        .map(this::toApiUserResponse)
        .orElseThrow(() -> new NoSuchElementException("User not found: " + userId));
  }

  public List<ApiUserResponse> listUsers() {
    return users.values().stream()
        .sorted((left, right) -> left.getCreatedAt().compareTo(right.getCreatedAt()))
        .map(this::toApiUserResponse)
        .toList();
  }

  public List<ApiUserResponse> searchUsers(String query) {
    if (query == null || query.isBlank()) {
      return listUsers();
    }
    String lowerQuery = query.toLowerCase();
    return users.values().stream()
        .filter(u ->
            u.getUserName().toLowerCase().contains(lowerQuery) ||
            (u.getEmail() != null && u.getEmail().toLowerCase().contains(lowerQuery)) ||
            (u.getFirstName() != null && u.getFirstName().toLowerCase().contains(lowerQuery)) ||
            (u.getLastName() != null && u.getLastName().toLowerCase().contains(lowerQuery)))
        .sorted((left, right) -> left.getCreatedAt().compareTo(right.getCreatedAt()))
        .map(this::toApiUserResponse)
        .toList();
  }

  public ApiUserResponse createUser(Map<String, Object> request) {
    String userName = extractString(request, "userName");
    if (userName.isBlank()) {
      throw new IllegalArgumentException("Username is required");
    }
    if (users.containsKey(userName.toLowerCase())) {
      throw new IllegalArgumentException("Username already exists: " + userName);
    }

    UserRecord user = new UserRecord();
    user.setId(UUID.randomUUID().toString());
    user.setUserName(userName);
    user.setFirstName(extractString(request, "firstName"));
    user.setLastName(extractString(request, "lastName"));
    user.setEmail(extractString(request, "email"));
    user.setDescription(extractString(request, "description"));
    user.setCreatedAt(Instant.now());
    user.setModifiedAt(Instant.now());
    user.setActive(true);
    user.setTag("user-" + Instant.now().toEpochMilli());
    user.setRoles(List.of(USER_ROLE));

    // 设置密码（如果提供）
    if (request.containsKey("password") && request.get("password") != null) {
      user.setPasswordHash(hashPassword(request.get("password").toString()));
    }

    users.put(user.getUserName().toLowerCase(), user);
    return toApiUserResponse(user);
  }

  public ApiUserResponse updateUser(String userName, Map<String, Object> request) {
    UserRecord user = users.get(userName.toLowerCase());
    if (user == null) {
      throw new NoSuchElementException("User not found: " + userName);
    }

    if (request.containsKey("firstName")) {
      user.setFirstName(extractString(request, "firstName"));
    }
    if (request.containsKey("lastName")) {
      user.setLastName(extractString(request, "lastName"));
    }
    if (request.containsKey("email")) {
      user.setEmail(extractString(request, "email"));
    }
    if (request.containsKey("description")) {
      user.setDescription(extractString(request, "description"));
    }
    if (request.containsKey("active")) {
      user.setActive(Boolean.TRUE.equals(request.get("active")));
    }
    if (request.containsKey("roles")) {
      @SuppressWarnings("unchecked")
      List<String> roles = (List<String>) request.get("roles");
      user.setRoles(roles != null ? new ArrayList<>(roles) : List.of());
    }
    if (request.containsKey("password") && request.get("password") != null) {
      user.setPasswordHash(hashPassword(request.get("password").toString()));
    }

    user.setModifiedAt(Instant.now());
    user.setTag("user-" + Instant.now().toEpochMilli());

    return toApiUserResponse(user);
  }

  public void deleteUser(String userName) {
    UserRecord user = users.get(userName.toLowerCase());
    if (user != null && DEFAULT_USER_ID.equals(user.getId())) {
      throw new IllegalArgumentException("Cannot delete the default user");
    }
    UserRecord removed = users.remove(userName.toLowerCase());
    if (removed == null) {
      throw new NoSuchElementException("User not found: " + userName);
    }
  }

  private ApiUserResponse toApiUserResponse(UserRecord user) {
    ApiUserResponse response = new ApiUserResponse();
    response.setId(user.getId());
    response.setName(user.getUserName());
    response.setFirstName(user.getFirstName());
    response.setLastName(user.getLastName());
    response.setEmail(user.getEmail());
    response.setTag(user.getTag());
    response.setActive(user.isActive());
    return response;
  }

  private String extractString(Map<String, Object> map, String key) {
    Object value = map.get(key);
    return value == null ? "" : value.toString();
  }

  private String hashPassword(String password) {
    // 简单的密码哈希（生产环境应使用 BCrypt 等）
    return "hash_" + Integer.toHexString(password.hashCode());
  }

  // 内部用户记录类
  public static class UserRecord {
    private String id;
    private String userName;
    private String firstName;
    private String lastName;
    private String email;
    private String description;
    private String passwordHash;
    private Instant createdAt;
    private Instant modifiedAt;
    private boolean active;
    private String tag;
    private List<String> roles;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getUserName() { return userName; }
    public void setUserName(String userName) { this.userName = userName; }
    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }
    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getPasswordHash() { return passwordHash; }
    public void setPasswordHash(String passwordHash) { this.passwordHash = passwordHash; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getModifiedAt() { return modifiedAt; }
    public void setModifiedAt(Instant modifiedAt) { this.modifiedAt = modifiedAt; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public String getTag() { return tag; }
    public void setTag(String tag) { this.tag = tag; }
    public List<String> getRoles() { return roles; }
    public void setRoles(List<String> roles) { this.roles = roles; }
  }
}
