package com.test.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * {@code OutputEvent} class represents processed event. Processed event is a result of applying of
 * several {@code InputEvent} with the same id.
 */
public class OutputEvent {
  public static long ALERT_THRESHOLD = 4;
  private static final Logger logger = LoggerFactory.getLogger(OutputEvent.class);
  private String id;
  private BigDecimal duration;
  private String type;
  private String host;
  private Boolean alert;

  private BigDecimal startedTimestamp;
  private BigDecimal finishedTimestamp;

  /** default empty constructor */
  public OutputEvent() {
    id = null;
    duration = null;
    type = null;
    host = null;
    alert = null;

    startedTimestamp = null;
    finishedTimestamp = null;
  }

  private void acceptId(String arg) {
    if (id == null) {
      id = arg;
    } else if (!id.equals(arg)) {
      throw new IllegalArgumentException("Only events with the same ID can be combined!");
    }
  }

  private void acceptStartedTimestamp(BigDecimal arg) {
    if (startedTimestamp == null) {
      startedTimestamp = arg;
    }
    if ((startedTimestamp != null) && (arg != null)) {
      if (startedTimestamp.compareTo(arg) > 0) {
        startedTimestamp = arg;
      }
    }
    updateDurationAndAlert();
  }

  private void acceptFinishedTimestamp(BigDecimal arg) {
    if (finishedTimestamp == null) {
      finishedTimestamp = arg;
    }
    if ((startedTimestamp != null) && (arg != null)) {
      if (finishedTimestamp.compareTo(arg) < 0) {
        finishedTimestamp = arg;
      }
    }
    updateDurationAndAlert();
  }

  private void updateDurationAndAlert() {
    if ((startedTimestamp != null) && (finishedTimestamp != null)) {
      duration = finishedTimestamp.subtract(startedTimestamp);
    } else {
      duration = null;
    }

    if (duration != null) {
      alert = duration.compareTo(BigDecimal.valueOf(OutputEvent.ALERT_THRESHOLD)) > 0;
    } else {
      alert = false;
    }
  }

  private void acceptHost(String arg) {
    if (host == null) {
      host = arg;
    }
  }

  private void acceptType(String arg) {
    if (type == null) {
      type = arg;
    }
  }

  /**
   * main method to apply data from {@code InputEvent} to the internal state.
   *
   * @param inputEvent source {@code InputEvent}
   * @return updated {@code OutputEvent}
   */
  public OutputEvent accept(InputEvent inputEvent) {
    logger.debug("Accepting: {}", inputEvent);
    acceptId(inputEvent.getId());

    if (inputEvent.getState().equals("STARTED")) {
      acceptStartedTimestamp(inputEvent.getTimestamp());
    }

    if (inputEvent.getState().equals("FINISHED")) {
      acceptFinishedTimestamp(inputEvent.getTimestamp());
    }

    acceptHost(inputEvent.getHost());

    acceptType(inputEvent.getType());

    logger.debug("Accepting result: {}", this);
    return this;
  }

  /**
   * Static method to merge two {@code OutputEvent} that were generated as result of applying {@code
   * InputEvent} in separate threads
   *
   * @param outputEvent1
   * @param outputEvent2
   * @return
   */
  public static OutputEvent combiner(OutputEvent outputEvent1, OutputEvent outputEvent2) {
    logger.debug("Combiner for 2 events: {} and {}", outputEvent1, outputEvent2);
    // return outputEvent1.combine(outputEvent2);
    outputEvent1.acceptId(outputEvent2.id);
    outputEvent1.acceptStartedTimestamp(outputEvent2.startedTimestamp);
    outputEvent1.acceptFinishedTimestamp(outputEvent2.finishedTimestamp);
    outputEvent1.acceptHost(outputEvent2.host);
    outputEvent1.acceptType(outputEvent2.type);
    logger.debug("Combined result: {}", outputEvent1);
    return outputEvent1;
  }

  @Override
  public String toString() {
    return String.format(
        "id=%s, duration=%s, type=%s, host=%s, alert=%s", id, duration, type, host, alert);
  }

  public String getId() {
    return id;
  }

  public BigDecimal getDuration() {
    return duration;
  }

  public String getType() {
    return type;
  }

  public String getHost() {
    return host;
  }

  public Boolean getAlert() {
    return alert;
  }
}
