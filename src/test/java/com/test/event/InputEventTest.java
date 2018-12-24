package com.test.event;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class InputEventTest {

  private InputEvent eventNotNull;
  private InputEvent eventNullable;

  @Before
  public void setUp() throws Exception {
    eventNotNull = new InputEvent("id1", "state2", BigDecimal.valueOf(3), "type4", "host5");
    eventNullable = new InputEvent("id1", "state2", BigDecimal.valueOf(3), null, null);
  }

  @Test
  public void testToStringNotNull() {
    String expected = "id=id1, state=state2, timestamp=3, type=type4, host=host5";
    String actual = eventNotNull.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void testToStringNullable() {
    String expected = "id=id1, state=state2, timestamp=3, type=null, host=null";
    String actual = eventNullable.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void getId() {
    String expected = "id1";
    String actual = eventNotNull.getId();
    assertEquals(expected, actual);
  }

  @Test
  public void getState() {
    String expected = "state2";
    String actual = eventNotNull.getState();
    assertEquals(expected, actual);
  }

  @Test
  public void getTimestamp() {
    BigDecimal expected = BigDecimal.valueOf(3);
    BigDecimal actual = eventNotNull.getTimestamp();
    assertEquals(expected, actual);
  }

  @Test
  public void getHostNotNull() {
    String expected = "host5";
    String actual = eventNotNull.getHost();
    assertEquals(expected, actual);
  }

  @Test
  public void getHostNullable() {
    String expected = null;
    String actual = eventNullable.getHost();
    assertEquals(expected, actual);
  }

  @Test
  public void getTypeNotNull() {
    String expected = "type4";
    String actual = eventNotNull.getType();
    assertEquals(expected, actual);
  }

  @Test
  public void getTypeNullable() {
    String expected = null;
    String actual = eventNullable.getType();
    assertEquals(expected, actual);
  }
}
