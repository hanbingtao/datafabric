package com.datafabric.controller;

import com.datafabric.dto.ApiUserResponse;
import com.datafabric.service.UserManagementService;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping({"/apiv2/users", "/api/v2/users"})
public class UserManagementController {
  private final UserManagementService userManagementService;

  public UserManagementController(UserManagementService userManagementService) {
    this.userManagementService = userManagementService;
  }

  @GetMapping({"", "/"})
  public List<ApiUserResponse> listUsers() {
    return userManagementService.listUsers();
  }

  @GetMapping({"/all", "/all/"})
  public List<ApiUserResponse> listAllUsers() {
    return userManagementService.listUsers();
  }

  @GetMapping({"/search", "/search/"})
  public List<ApiUserResponse> searchUsers(@RequestParam(required = false) String filter) {
    return userManagementService.searchUsers(filter);
  }

  @GetMapping({"/{userName}", "/{userName}/"})
  public ApiUserResponse getUser(@PathVariable String userName) {
    return userManagementService.getUserByName(userName);
  }

  @PostMapping({"", "/"})
  public ApiUserResponse createUser(@RequestBody(required = false) Map<String, Object> request) {
    return userManagementService.createUser(request == null ? Map.of() : request);
  }

  @PutMapping({"/{userName}", "/{userName}/"})
  public ApiUserResponse updateUser(
      @PathVariable String userName, @RequestBody(required = false) Map<String, Object> request) {
    return userManagementService.updateUser(userName, request == null ? Map.of() : request);
  }

  @DeleteMapping({"/{userName}", "/{userName}/"})
  public void deleteUser(@PathVariable String userName) {
    userManagementService.deleteUser(userName);
  }
}
