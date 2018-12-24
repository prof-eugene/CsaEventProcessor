package com.test.event;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class OutputEventTest {
  OutputEvent outputEventEmpty;
  OutputEvent outputEvent1;
  OutputEvent outputEvent2;

  InputEvent startedInputEvent1;
  InputEvent finishedInputEvent1;

  InputEvent startedInputEvent2;
  InputEvent finishedInputEvent2;

  @Before
  public void setUp() throws Exception {
    OutputEvent.ALERT_THRESHOLD = 4;
    outputEventEmpty = new OutputEvent();

    startedInputEvent1 = new InputEvent("id1", "STARTED", BigDecimal.valueOf(3), "type", "host");
    finishedInputEvent1 = new InputEvent("id1", "FINISHED", BigDecimal.valueOf(8), "type", "host");

    startedInputEvent2 = new InputEvent("id2", "STARTED", BigDecimal.valueOf(3), null, null);
    finishedInputEvent2 = new InputEvent("id2", "FINISHED", BigDecimal.valueOf(4), null, null);

    startedInputEvent1 = new InputEvent("id1", "STARTED", BigDecimal.valueOf(3), "type", "host");
    finishedInputEvent1 = new InputEvent("id1", "FINISHED", BigDecimal.valueOf(8), "type", "host");
  }

  @Test
  public void acceptOnlyStarted1() {
    String expected = "id=id1, duration=null, type=type, host=host, alert=false";
    String actual = outputEventEmpty.accept(startedInputEvent1).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptOnlyStarted2() {
    String expected = "id=id2, duration=null, type=null, host=null, alert=false";
    String actual = outputEventEmpty.accept(startedInputEvent2).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptOnlyFinished1() {
    String expected = "id=id1, duration=null, type=type, host=host, alert=false";
    String actual = outputEventEmpty.accept(finishedInputEvent1).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptOnlyFinished2() {
    String expected = "id=id2, duration=null, type=null, host=null, alert=false";
    String actual = outputEventEmpty.accept(finishedInputEvent2).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptBoth1() {
    String expected = "id=id1, duration=5, type=type, host=host, alert=true";
    String actual =
        outputEventEmpty.accept(startedInputEvent1).accept(finishedInputEvent1).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptBoth2() {
    String expected = "id=id2, duration=1, type=null, host=null, alert=false";
    String actual =
        outputEventEmpty.accept(startedInputEvent2).accept(finishedInputEvent2).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptBothReverse1() {
    String expected = "id=id1, duration=5, type=type, host=host, alert=true";
    String actual =
        outputEventEmpty.accept(finishedInputEvent1).accept(startedInputEvent1).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptBothReverse2() {
    String expected = "id=id2, duration=1, type=null, host=null, alert=false";
    String actual =
        outputEventEmpty.accept(finishedInputEvent2).accept(startedInputEvent2).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptStartedEarlier1() {
    String expected = "id=, duration=4, type=, host=, alert=false";
    String actual =
        outputEventEmpty
            .accept(new InputEvent("", "STARTED", BigDecimal.valueOf(1), "", ""))
            .accept(new InputEvent("", "STARTED", BigDecimal.valueOf(2), "", ""))
            .accept(new InputEvent("", "FINISHED", BigDecimal.valueOf(5), "", ""))
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptStartedEarlier2() {
    String expected = "id=, duration=4, type=, host=, alert=false";
    String actual =
        outputEventEmpty
            .accept(new InputEvent("", "STARTED", BigDecimal.valueOf(2), "", ""))
            .accept(new InputEvent("", "STARTED", BigDecimal.valueOf(1), "", ""))
            .accept(new InputEvent("", "FINISHED", BigDecimal.valueOf(5), "", ""))
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptFinishedLater1() {
    String expected = "id=, duration=4, type=, host=, alert=false";
    String actual =
        outputEventEmpty
            .accept(new InputEvent("", "STARTED", BigDecimal.valueOf(1), "", ""))
            .accept(new InputEvent("", "FINISHED", BigDecimal.valueOf(4), "", ""))
            .accept(new InputEvent("", "FINISHED", BigDecimal.valueOf(5), "", ""))
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void acceptFinishedLater2() {
    String expected = "id=, duration=4, type=, host=, alert=false";
    String actual =
        outputEventEmpty
            .accept(new InputEvent("", "STARTED", BigDecimal.valueOf(1), "", ""))
            .accept(new InputEvent("", "FINISHED", BigDecimal.valueOf(5), "", ""))
            .accept(new InputEvent("", "FINISHED", BigDecimal.valueOf(4), "", ""))
            .toString();
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void acceptDifferentId1() {
    String expected = "";
    String actual =
        outputEventEmpty
            .accept(new InputEvent("id1", "STARTED", BigDecimal.valueOf(1), "", ""))
            .accept(new InputEvent("id2", "FINISHED", BigDecimal.valueOf(5), "", ""))
            .toString();
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void acceptDifferentId2() {
    String expected = "";
    String actual =
        outputEventEmpty
            .accept(new InputEvent("id1", "FINISHED", BigDecimal.valueOf(1), "", ""))
            .accept(new InputEvent("id2", "STARTED", BigDecimal.valueOf(5), "", ""))
            .toString();
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void acceptDifferentId3() {
    String expected = "";
    String actual =
        outputEventEmpty
            .accept(new InputEvent("id1", "STARTED", BigDecimal.valueOf(1), "", ""))
            .accept(new InputEvent("id2", "STARTED", BigDecimal.valueOf(5), "", ""))
            .toString();
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void acceptDifferentId4() {
    String expected = "";
    String actual =
        outputEventEmpty
            .accept(new InputEvent("id1", "FINISHED", BigDecimal.valueOf(1), "", ""))
            .accept(new InputEvent("id2", "FINISHED", BigDecimal.valueOf(5), "", ""))
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void combine1() {
    String expected = "id=id1, duration=5, type=type, host=host, alert=true";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent1, outputEvent2);
    String actual = combinedOutputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void combineReverse1() {
    String expected = "id=id1, duration=5, type=type, host=host, alert=true";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent2, outputEvent1);
    String actual = combinedOutputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void combine2() {
    String expected = "id=id2, duration=1, type=null, host=null, alert=false";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent2);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent2);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent1, outputEvent2);
    String actual = combinedOutputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void combineReverse2() {
    String expected = "id=id2, duration=1, type=null, host=null, alert=false";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent2);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent2);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent2, outputEvent1);
    String actual = combinedOutputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void combiner1() {
    String expected = "id=id1, duration=5, type=type, host=host, alert=true";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent1, outputEvent2);
    String actual = combinedOutputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void combinerReverse1() {
    String expected = "id=id1, duration=5, type=type, host=host, alert=true";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent2, outputEvent1);
    String actual = combinedOutputEvent.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void testToStringEmpty() {
    String expected = "id=null, duration=null, type=null, host=null, alert=null";
    String actual = outputEventEmpty.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void testToStringAccept() {
    String expected = "id=null, duration=null, type=null, host=null, alert=null";
    String actual = outputEventEmpty.toString();
    assertEquals(expected, actual);
  }

  @Test
  public void getId() {
    String expected = "id1";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent1, outputEvent2);
    String actual = combinedOutputEvent.getId();
    assertEquals(expected, actual);
  }

  @Test
  public void getDuration() {
    String expected = "5";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent1, outputEvent2);
    String actual = combinedOutputEvent.getDuration().toString();
    assertEquals(expected, actual);
  }

  @Test
  public void getType() {
    String expected = "type";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent1, outputEvent2);
    String actual = combinedOutputEvent.getType();
    assertEquals(expected, actual);
  }

  @Test
  public void getHost() {
    String expected = "host";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent1, outputEvent2);
    String actual = combinedOutputEvent.getHost();
    assertEquals(expected, actual);
  }

  @Test
  public void getAlert() {
    String expected = "true";
    OutputEvent outputEvent1 = new OutputEvent().accept(startedInputEvent1);
    OutputEvent outputEvent2 = new OutputEvent().accept(finishedInputEvent1);
    OutputEvent combinedOutputEvent = OutputEvent.combiner(outputEvent1, outputEvent2);
    String actual = combinedOutputEvent.getAlert().toString();
    assertEquals(expected, actual);
  }
}
