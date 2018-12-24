package com.test.event;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InputEventFactoryTest {


  @Test
  public void buildEvent1() {
    InputEvent inputEvent =
        InputEventFactory.buildEvent(
            "{\"id\":\"scsmbstgra\", \"state\":\"STARTED\", \"type\":\"APPLICATION_LOG\",\"host\":\"12345\", \"timestamp\":1491377495212}");
    String expected =
        "id=scsmbstgra, state=STARTED, timestamp=1491377495212, type=APPLICATION_LOG, host=12345";
    String actual = inputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void buildEvent2() {
    InputEvent inputEvent =
        InputEventFactory.buildEvent(
            "{\"id\":\"scsmbstgrb\", \"state\":\"STARTED\", \"timestamp\":1491377495213}");
    String expected = "id=scsmbstgrb, state=STARTED, timestamp=1491377495213, type=null, host=null";
    String actual = inputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void buildEvent3() {
    InputEvent inputEvent =
        InputEventFactory.buildEvent(
            "{\"id\":\"scsmbstgrc\", \"state\":\"FINISHED\", \"timestamp\":1491377495218}");
    String expected =
        "id=scsmbstgrc, state=FINISHED, timestamp=1491377495218, type=null, host=null";
    String actual = inputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void buildEvent4() {
    InputEvent inputEvent =
        InputEventFactory.buildEvent(
            "{\"id\":\"scsmbstgra\", \"state\":\"FINISHED\", \"type\":\"APPLICATION_LOG\",\"host\":\"12345\", \"timestamp\":1491377495217}");
    String expected =
        "id=scsmbstgra, state=FINISHED, timestamp=1491377495217, type=APPLICATION_LOG, host=12345";
    String actual = inputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void buildEvent5() {
    InputEvent inputEvent =
        InputEventFactory.buildEvent(
            "{\"id\":\"scsmbstgrc\", \"state\":\"STARTED\", \"timestamp\":1491377495210}");
    String expected = "id=scsmbstgrc, state=STARTED, timestamp=1491377495210, type=null, host=null";
    String actual = inputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void buildEvent6() {
    InputEvent inputEvent =
        InputEventFactory.buildEvent(
            "{\"id\":\"scsmbstgrb\", \"state\":\"FINISHED\", \"timestamp\":1491377495216}");
    String expected =
        "id=scsmbstgrb, state=FINISHED, timestamp=1491377495216, type=null, host=null";
    String actual = inputEvent.toString();
    assertEquals(expected, actual);
  }
}
