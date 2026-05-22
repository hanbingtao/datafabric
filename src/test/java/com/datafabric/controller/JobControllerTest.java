package com.datafabric.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest
@AutoConfigureMockMvc
class JobControllerTest {

  @Autowired
  private MockMvc mockMvc;

  @Test
  void getJobDataRejectsNegativeOffset() throws Exception {
    String jobId = submitJob();

    mockMvc.perform(get("/api/v2/job/{jobId}/data", jobId).param("offset", "-1"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.errorCode").value("BAD_REQUEST"))
        .andExpect(jsonPath("$.errorMessage").value("offset must be greater than or equal to 0"));
  }

  @Test
  void getJobDataRejectsNonPositiveLimit() throws Exception {
    String jobId = submitJob();

    mockMvc.perform(get("/api/v2/job/{jobId}/data", jobId).param("limit", "0"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.errorCode").value("BAD_REQUEST"))
        .andExpect(jsonPath("$.errorMessage").value("limit must be greater than 0"));
  }

  @Test
  void dremioV3JobResultsReturnDocumentedShapeWithDefaultPagination() throws Exception {
    String jobId = submitJob();

    mockMvc.perform(get("/api/v3/job/{jobId}/results", jobId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.rowCount").value(3))
        .andExpect(jsonPath("$.schema", Matchers.hasSize(4)))
        .andExpect(jsonPath("$.schema[0].name").value("ID"))
        .andExpect(jsonPath("$.schema[0].type.name").value("ANY"))
        .andExpect(jsonPath("$.rows", Matchers.hasSize(3)))
        .andExpect(jsonPath("$.rows[0].CUSTOMER").value("alice"));
  }

  @Test
  void dremioV3JobResultsApplyOffsetAndLimit() throws Exception {
    String jobId = submitJob();

    mockMvc.perform(get("/api/v3/job/{jobId}/results", jobId)
            .param("offset", "1")
            .param("limit", "1"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.rowCount").value(3))
        .andExpect(jsonPath("$.rows", Matchers.hasSize(1)))
        .andExpect(jsonPath("$.rows[0].CUSTOMER").value("bob"));
  }

  @Test
  void dremioV3JobResultsRejectLimitAboveDocumentedMaximum() throws Exception {
    String jobId = submitJob();

    mockMvc.perform(get("/api/v3/job/{jobId}/results", jobId).param("limit", "501"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.errorCode").value("BAD_REQUEST"))
        .andExpect(jsonPath("$.errorMessage").value("limit must be less than or equal to 500"));
  }

  private String submitJob() throws Exception {
    MvcResult result =
        mockMvc.perform(post("/api/v3/sql")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {"sql":"select * from SALES_FACT"}
                    """))
            .andExpect(status().isOk())
            .andReturn();

    String content = result.getResponse().getContentAsString();
    int prefixLength = "{\"id\":\"".length();
    return content.substring(prefixLength, content.length() - 2);
  }
}
