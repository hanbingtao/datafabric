package com.datafabric.dto;

import java.util.ArrayList;
import java.util.List;

public class JobAndUserStatsResponse {
  private String edition;
  private List<Stat> stats = new ArrayList<>();
  private List<JobStat> jobStats = new ArrayList<>();
  private List<UserStat> userStatsByDate = new ArrayList<>();
  private List<UserStat> userStatsByWeek = new ArrayList<>();
  private List<UserStat> userStatsByMonth = new ArrayList<>();

  public String getEdition() {
    return edition;
  }

  public void setEdition(String edition) {
    this.edition = edition;
  }

  public List<Stat> getStats() {
    return stats;
  }

  public void setStats(List<Stat> stats) {
    this.stats = stats;
  }

  public List<JobStat> getJobStats() {
    return jobStats;
  }

  public void setJobStats(List<JobStat> jobStats) {
    this.jobStats = jobStats;
  }

  public List<UserStat> getUserStatsByDate() {
    return userStatsByDate;
  }

  public void setUserStatsByDate(List<UserStat> userStatsByDate) {
    this.userStatsByDate = userStatsByDate;
  }

  public List<UserStat> getUserStatsByWeek() {
    return userStatsByWeek;
  }

  public void setUserStatsByWeek(List<UserStat> userStatsByWeek) {
    this.userStatsByWeek = userStatsByWeek;
  }

  public List<UserStat> getUserStatsByMonth() {
    return userStatsByMonth;
  }

  public void setUserStatsByMonth(List<UserStat> userStatsByMonth) {
    this.userStatsByMonth = userStatsByMonth;
  }

  public static class Stat {
    private String date;
    private Long jobCount;
    private Long uniqueUsersCount;

    public Stat() {}

    public Stat(String date, Long jobCount, Long uniqueUsersCount) {
      this.date = date;
      this.jobCount = jobCount;
      this.uniqueUsersCount = uniqueUsersCount;
    }

    public String getDate() {
      return date;
    }

    public void setDate(String date) {
      this.date = date;
    }

    public Long getJobCount() {
      return jobCount;
    }

    public void setJobCount(Long jobCount) {
      this.jobCount = jobCount;
    }

    public Long getUniqueUsersCount() {
      return uniqueUsersCount;
    }

    public void setUniqueUsersCount(Long uniqueUsersCount) {
      this.uniqueUsersCount = uniqueUsersCount;
    }
  }

  public static class JobStat {
    private String date;
    private Long total;
    private Long UI_RUN;
    private Long REST;

    public JobStat() {}

    public JobStat(String date, Long total, Long uiRun, Long rest) {
      this.date = date;
      this.total = total;
      this.UI_RUN = uiRun;
      this.REST = rest;
    }

    public String getDate() {
      return date;
    }

    public void setDate(String date) {
      this.date = date;
    }

    public Long getTotal() {
      return total;
    }

    public void setTotal(Long total) {
      this.total = total;
    }

    public Long getUI_RUN() {
      return UI_RUN;
    }

    public void setUI_RUN(Long UI_RUN) {
      this.UI_RUN = UI_RUN;
    }

    public Long getREST() {
      return REST;
    }

    public void setREST(Long REST) {
      this.REST = REST;
    }
  }

  public static class UserStat {
    private String date;
    private Long total;
    private Long UI_RUN;
    private Long REST;

    public UserStat() {}

    public UserStat(String date, Long total, Long uiRun, Long rest) {
      this.date = date;
      this.total = total;
      this.UI_RUN = uiRun;
      this.REST = rest;
    }

    public String getDate() {
      return date;
    }

    public void setDate(String date) {
      this.date = date;
    }

    public Long getTotal() {
      return total;
    }

    public void setTotal(Long total) {
      this.total = total;
    }

    public Long getUI_RUN() {
      return UI_RUN;
    }

    public void setUI_RUN(Long UI_RUN) {
      this.UI_RUN = UI_RUN;
    }

    public Long getREST() {
      return REST;
    }

    public void setREST(Long REST) {
      this.REST = REST;
    }
  }
}
