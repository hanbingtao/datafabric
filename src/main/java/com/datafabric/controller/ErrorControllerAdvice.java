package com.datafabric.controller;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ErrorControllerAdvice {

  @ExceptionHandler(NoSuchElementException.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public Map<String, Object> handleNotFound(NoSuchElementException ex) {
    return error("NOT_FOUND", ex.getMessage());
  }

  @ExceptionHandler({IllegalStateException.class, IllegalArgumentException.class})
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public Map<String, Object> handleBadRequest(RuntimeException ex) {
    return error("BAD_REQUEST", ex.getMessage());
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public Map<String, Object> handleValidation(MethodArgumentNotValidException ex) {
    String message =
        ex.getBindingResult().getAllErrors().isEmpty()
            ? "Validation failed"
            : ex.getBindingResult().getAllErrors().get(0).getDefaultMessage();
    return error("VALIDATION_ERROR", message);
  }

  @ExceptionHandler(NoResourceFoundException.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public Map<String, Object> handleStaticNotFound(NoResourceFoundException ex) {
    return error("NOT_FOUND", ex.getMessage());
  }

  @ExceptionHandler(ResponseStatusException.class)
  @ResponseStatus(HttpStatus.NOT_FOUND)
  public Map<String, Object> handleResponseStatus(ResponseStatusException ex) {
    return error("NOT_FOUND", ex.getReason());
  }

  @ExceptionHandler(Exception.class)
  @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
  public Map<String, Object> handleGeneric(Exception ex) {
    String message = ex.getMessage() + " [" + ex.getClass().getName() + "]";
    if (ex.getCause() != null) {
      message += " <- " + ex.getCause().getMessage();
    }
    return error("INTERNAL_ERROR", message);
  }

  private Map<String, Object> error(String code, String message) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("errorCode", code);
    body.put("errorMessage", message);
    return body;
  }
}
