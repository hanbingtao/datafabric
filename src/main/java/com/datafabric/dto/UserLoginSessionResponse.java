package com.datafabric.dto;

public class UserLoginSessionResponse {
  private String token;
  private String userName;
  private String firstName;
  private String lastName;
  private long expires;
  private String email;
  private String userId;
  private boolean admin;
  private long createdAt;
  private String clusterId;
  private long clusterCreatedAt;
  private String version;
  private SessionPermissions permissions;

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public long getExpires() {
    return expires;
  }

  public void setExpires(long expires) {
    this.expires = expires;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public boolean isAdmin() {
    return admin;
  }

  public void setAdmin(boolean admin) {
    this.admin = admin;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  public long getClusterCreatedAt() {
    return clusterCreatedAt;
  }

  public void setClusterCreatedAt(long clusterCreatedAt) {
    this.clusterCreatedAt = clusterCreatedAt;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public SessionPermissions getPermissions() {
    return permissions;
  }

  public void setPermissions(SessionPermissions permissions) {
    this.permissions = permissions;
  }

  public static class SessionPermissions {
    private boolean canUploadProfiles = true;
    private boolean canDownloadProfiles = true;
    private boolean canEmailForSupport = false;
    private boolean canChatForSupport = false;
    private boolean canViewJobs = true;
    private boolean canCreateUser = true;
    private boolean canCreateRole = true;
    private boolean canCreateSource = true;
    private boolean canUploadFile = true;
    private boolean canManageNodeActivity = true;
    private boolean canManageEngines = true;
    private boolean canManageQueues = true;
    private boolean canManageReflections = true;
    private boolean canManageSupportSettings = true;
    private boolean canConfigureSecurity = true;
    private boolean canRunDiagnostic = true;

    public boolean isCanUploadProfiles() {
      return canUploadProfiles;
    }

    public void setCanUploadProfiles(boolean canUploadProfiles) {
      this.canUploadProfiles = canUploadProfiles;
    }

    public boolean isCanDownloadProfiles() {
      return canDownloadProfiles;
    }

    public void setCanDownloadProfiles(boolean canDownloadProfiles) {
      this.canDownloadProfiles = canDownloadProfiles;
    }

    public boolean isCanEmailForSupport() {
      return canEmailForSupport;
    }

    public void setCanEmailForSupport(boolean canEmailForSupport) {
      this.canEmailForSupport = canEmailForSupport;
    }

    public boolean isCanChatForSupport() {
      return canChatForSupport;
    }

    public void setCanChatForSupport(boolean canChatForSupport) {
      this.canChatForSupport = canChatForSupport;
    }

    public boolean isCanViewJobs() {
      return canViewJobs;
    }

    public void setCanViewJobs(boolean canViewJobs) {
      this.canViewJobs = canViewJobs;
    }

    public boolean isCanCreateUser() {
      return canCreateUser;
    }

    public void setCanCreateUser(boolean canCreateUser) {
      this.canCreateUser = canCreateUser;
    }

    public boolean isCanCreateRole() {
      return canCreateRole;
    }

    public void setCanCreateRole(boolean canCreateRole) {
      this.canCreateRole = canCreateRole;
    }

    public boolean isCanCreateSource() {
      return canCreateSource;
    }

    public void setCanCreateSource(boolean canCreateSource) {
      this.canCreateSource = canCreateSource;
    }

    public boolean isCanUploadFile() {
      return canUploadFile;
    }

    public void setCanUploadFile(boolean canUploadFile) {
      this.canUploadFile = canUploadFile;
    }

    public boolean isCanManageNodeActivity() {
      return canManageNodeActivity;
    }

    public void setCanManageNodeActivity(boolean canManageNodeActivity) {
      this.canManageNodeActivity = canManageNodeActivity;
    }

    public boolean isCanManageEngines() {
      return canManageEngines;
    }

    public void setCanManageEngines(boolean canManageEngines) {
      this.canManageEngines = canManageEngines;
    }

    public boolean isCanManageQueues() {
      return canManageQueues;
    }

    public void setCanManageQueues(boolean canManageQueues) {
      this.canManageQueues = canManageQueues;
    }

    public boolean isCanManageReflections() {
      return canManageReflections;
    }

    public void setCanManageReflections(boolean canManageReflections) {
      this.canManageReflections = canManageReflections;
    }

    public boolean isCanManageSupportSettings() {
      return canManageSupportSettings;
    }

    public void setCanManageSupportSettings(boolean canManageSupportSettings) {
      this.canManageSupportSettings = canManageSupportSettings;
    }

    public boolean isCanConfigureSecurity() {
      return canConfigureSecurity;
    }

    public void setCanConfigureSecurity(boolean canConfigureSecurity) {
      this.canConfigureSecurity = canConfigureSecurity;
    }

    public boolean isCanRunDiagnostic() {
      return canRunDiagnostic;
    }

    public void setCanRunDiagnostic(boolean canRunDiagnostic) {
      this.canRunDiagnostic = canRunDiagnostic;
    }
  }
}
