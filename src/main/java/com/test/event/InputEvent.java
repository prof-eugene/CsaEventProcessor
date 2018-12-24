package com.test.event;

import java.math.BigDecimal;

/** {@code InputEvent class represents data of an input event} */
public class InputEvent {

  private final String id;
  private final String state;
  private final BigDecimal timestamp; // TODO can be long, if certain in datatype!
  private final String type;
  private final String host;

  public InputEvent(String id, String state, BigDecimal timestamp, String type, String host) {
    this.id = id;
    this.state = state;
    this.timestamp = timestamp;
    this.type = type;
    this.host = host;
  }

  @Override
  public String toString() {
    return String.format(
        "id=%s, state=%s, timestamp=%s, type=%s, host=%s", id, state, timestamp, type, host);
  }

  public String getId() {
    return id;
  }

  public String getState() {
    return this.state;
  }

  public BigDecimal getTimestamp() {
    return timestamp;
  }

  public String getHost() {
    return host;
  }

  public String getType() {
    return type;
  }
}
