package com.datafabric.service;

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class SqlTemplateService {
  
  private final List<SqlTemplate> templates = new ArrayList<>();

  @PostConstruct
  void init() {
    // 添加默认 SQL 模板
    templates.add(new SqlTemplate(
        "sales_summary",
        "Sales Summary",
        "Aggregates sales data by region",
        "SELECT region, SUM(amount) as total_sales, COUNT(*) as order_count\nFROM ${table}\nGROUP BY region\nORDER BY total_sales DESC",
        List.of("table")
    ));
    
    templates.add(new SqlTemplate(
        "top_customers",
        "Top Customers",
        "Find top N customers by order value",
        "SELECT customer_id, SUM(amount) as total_spent\nFROM ${table}\nGROUP BY customer_id\nORDER BY total_spent DESC\nLIMIT ${limit:10}",
        List.of("table", "limit")
    ));
    
    templates.add(new SqlTemplate(
        "daily_trend",
        "Daily Trend",
        "Analyze daily trends",
        "SELECT DATE_TRUNC('day', created_at) as date,\n       COUNT(*) as transactions,\n       SUM(amount) as total_amount\nFROM ${table}\nWHERE created_at >= DATE_TRUNC('day', CURRENT_DATE - INTERVAL '${days:30} day')\nGROUP BY DATE_TRUNC('day', created_at)\nORDER BY date",
        List.of("table", "days")
    ));
    
    templates.add(new SqlTemplate(
        "category_breakdown",
        "Category Breakdown",
        "Show breakdown by category",
        "SELECT category,\n       COUNT(*) as count,\n       AVG(price) as avg_price,\n       SUM(quantity) as total_quantity\nFROM ${table}\nGROUP BY category\nORDER BY count DESC",
        List.of("table")
    ));
    
    templates.add(new SqlTemplate(
        "recent_records",
        "Recent Records",
        "Get most recent N records",
        "SELECT *\nFROM ${table}\nORDER BY created_at DESC\nLIMIT ${limit:50}",
        List.of("table", "limit")
    ));
    
    templates.add(new SqlTemplate(
        "null_check",
        "Null Value Check",
        "Find records with null values in specified columns",
        "SELECT *\nFROM ${table}\nWHERE ${column} IS NULL\nLIMIT ${limit:100}",
        List.of("table", "column", "limit")
    ));
    
    templates.add(new SqlTemplate(
        "duplicate_check",
        "Duplicate Records",
        "Find duplicate records based on key columns",
        "SELECT ${key_column}, COUNT(*) as duplicate_count\nFROM ${table}\nGROUP BY ${key_column}\nHAVING COUNT(*) > 1",
        List.of("table", "key_column")
    ));
    
    templates.add(new SqlTemplate(
        "data_comparison",
        "Compare Periods",
        "Compare data between two time periods",
        "SELECT \n  DATE_TRUNC('day', created_at) as date,\n  SUM(CASE WHEN created_at >= DATE_TRUNC('day', CURRENT_DATE - INTERVAL '${period:7} day') \n           AND created_at < DATE_TRUNC('day', CURRENT_DATE) \n           THEN amount ELSE 0 END) as current_period,\n  SUM(CASE WHEN created_at >= DATE_TRUNC('day', CURRENT_DATE - INTERVAL '${period:7} day' * 2) \n           AND created_at < DATE_TRUNC('day', CURRENT_DATE - INTERVAL '${period:7} day') \n           THEN amount ELSE 0 END) as previous_period\nFROM ${table}\nGROUP BY DATE_TRUNC('day', created_at)\nORDER BY date",
        List.of("table", "period")
    ));
    
    templates.add(new SqlTemplate(
        "running_total",
        "Running Total",
        "Calculate running total",
        "SELECT date,\n       value,\n       SUM(value) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) as running_total\nFROM ${table}\nORDER BY date",
        List.of("table")
    ));
    
    templates.add(new SqlTemplate(
        "percentile_analysis",
        "Percentile Analysis",
        "Calculate percentile distribution",
        "SELECT \n  '${percentile:50}th percentile' as percentile,\n  AVG(CASE WHEN row_num <= CEIL(COUNT(*) * ${percentile:50} / 100.0) THEN value END) as percentile_value\nFROM (\n  SELECT value, ROW_NUMBER() OVER (ORDER BY value) as row_num, COUNT(*) OVER () as total_count\n  FROM ${table}\n) ranked",
        List.of("table", "percentile")
    ));
  }

  public List<SqlTemplate> listTemplates() {
    return new ArrayList<>(templates);
  }

  public List<SqlTemplate> searchTemplates(String query) {
    if (query == null || query.isBlank()) {
      return listTemplates();
    }
    String lowerQuery = query.toLowerCase();
    return templates.stream()
        .filter(t -> 
            t.name.toLowerCase().contains(lowerQuery) ||
            t.description.toLowerCase().contains(lowerQuery) ||
            t.sql.toLowerCase().contains(lowerQuery))
        .toList();
  }

  public SqlTemplate getTemplate(String templateId) {
    return templates.stream()
        .filter(t -> t.id.equals(templateId))
        .findFirst()
        .orElse(null);
  }

  public String renderTemplate(String templateId, Map<String, String> parameters) {
    SqlTemplate template = getTemplate(templateId);
    if (template == null) {
      return null;
    }
    
    String rendered = template.sql;
    for (String param : template.parameters) {
      String value = parameters.get(param);
      if (value != null) {
        rendered = rendered.replace("${" + param + "}", value);
        rendered = rendered.replace("${" + param + ":#default}", value);
      } else {
        // 处理带默认值的参数
        int startIdx = rendered.indexOf("${" + param + ":");
        while (startIdx >= 0) {
          int endIdx = rendered.indexOf("}", startIdx);
          if (endIdx > startIdx) {
            String defaultValue = rendered.substring(startIdx + param.length() + 3, endIdx);
            rendered = rendered.substring(0, startIdx) + defaultValue + rendered.substring(endIdx + 1);
          }
          startIdx = rendered.indexOf("${" + param + ":", endIdx);
        }
        // 移除没有默认值的参数占位符
        rendered = rendered.replace("${" + param + "}", "");
      }
    }
    return rendered;
  }

  public SqlTemplate createTemplate(String name, String description, String sql, List<String> parameters) {
    SqlTemplate template = new SqlTemplate(
        "custom_" + System.currentTimeMillis(),
        name,
        description,
        sql,
        parameters
    );
    templates.add(template);
    return template;
  }

  public boolean deleteTemplate(String templateId) {
    return templates.removeIf(t -> t.id.equals(templateId) && t.id.startsWith("custom_"));
  }

  // SQL 模板记录
  public static class SqlTemplate {
    private final String id;
    private final String name;
    private final String description;
    private final String sql;
    private final List<String> parameters;

    public SqlTemplate(String id, String name, String description, String sql, List<String> parameters) {
      this.id = id;
      this.name = name;
      this.description = description;
      this.sql = sql;
      this.parameters = parameters;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public String getDescription() { return description; }
    public String getSql() { return sql; }
    public List<String> getParameters() { return parameters; }
  }
}
