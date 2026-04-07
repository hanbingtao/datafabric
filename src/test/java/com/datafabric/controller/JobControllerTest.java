package com.datafabric.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
