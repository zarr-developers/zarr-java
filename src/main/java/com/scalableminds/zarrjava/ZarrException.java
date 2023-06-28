package com.scalableminds.zarrjava;

public class ZarrException extends Exception {

  public ZarrException(String message, Throwable cause) {
    super(message, cause);
  }

  public ZarrException(String message) {
    super(message);
  }
}
