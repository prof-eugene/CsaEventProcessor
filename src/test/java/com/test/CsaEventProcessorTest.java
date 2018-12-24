package com.test;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CsaEventProcessorTest {
  private static final String INPUT_FILE_NAME =
      System.getProperty("user.dir")
          + "/src/test/resources/input.log".replaceAll("/", File.separator);
  private static final String BIG_INPUT_FILE_NAME = "/src/test/resources/big_input.log";
  private static final String BIG_TEMPLATE_FILE_NAME = "/src/test/resources/big_template.log";
  private static final String OUTPUT_FILE_NAME =
      "src/test/resources/output.csv".replaceAll("/", File.separator);
  private static final String BIG_OUTPUT_FILE_NAME =
      "src/test/resources/big_output.csv".replaceAll("/", File.separator);
  private static final String ETALON_OUTPUT_FILE_NAME =
      "src/test/resources/etalon_output.csv".replaceAll("/", File.separator);
  private static final String WORK_DB_URI =
      "jdbc:hsqldb:file:"
          + System.getProperty("user.dir")
          + File.separator
          + "testdb"
          + ";shutdown=true";

  private CsaEventProcessor app;

  @Before
  public void setUp() {
    app = new CsaEventProcessor();
  }

  @Test
  public void getInputStringStreamFilter1() throws IOException {
    String expected =
        "{\"id\":\"scsmbstgra\", \"state\":\"STARTED\", \"type\":\"APPLICATION_LOG\",\"host\":\"12345\", \"timestamp\":1491377495212}";
    String actual =
        app.getInputStringStream(INPUT_FILE_NAME)
            .filter(s -> s.contains("\"id\":\"scsmbstgra\""))
            .filter(s -> s.contains("STARTED"))
            .findAny()
            .get();
    assertEquals(expected, actual);
  }

  @Test
  public void getInputStringStreamFilter2() throws IOException {
    String expected =
        "{\"id\":\"scsmbstgrb\", \"state\":\"FINISHED\", \"timestamp\":1491377495216}";
    String actual =
        app.getInputStringStream(INPUT_FILE_NAME)
            .filter(s -> s.contains("\"id\":\"scsmbstgrb\""))
            .filter(s -> s.contains("FINISHED"))
            .findAny()
            .get();
    assertEquals(expected, actual);
  }

  @Test
  public void getInputStringStreamCount() throws IOException {
    long expected = 6;
    long actual = app.getInputStringStream(INPUT_FILE_NAME).count();
    assertEquals(expected, actual);
  }

  @Test
  public void getInputEventStreamFilter1() throws IOException {
    String expected =
        "id=scsmbstgra, state=STARTED, timestamp=1491377495212, type=APPLICATION_LOG, host=12345";
    String actual =
        app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME))
            .filter(e -> e.getId().equals("scsmbstgra"))
            .filter(e -> e.getState().equals("STARTED"))
            .findAny()
            .get()
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void getInputEventStreamFilter2() throws IOException {
    String expected =
        "id=scsmbstgrb, state=FINISHED, timestamp=1491377495216, type=null, host=null";
    String actual =
        app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME))
            .filter(e -> e.getId().equals("scsmbstgrb"))
            .filter(e -> e.getState().equals("FINISHED"))
            .findAny()
            .get()
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void getInputEventStreamCount() throws IOException {
    long expected = 6;
    long actual = app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME)).count();
    assertEquals(expected, actual);
  }

  @Test
  public void getOutputEventsStreamFilter1() throws IOException {
    String expected = "id=scsmbstgra, duration=5, type=APPLICATION_LOG, host=12345, alert=true";
    String actual =
        app.getOutputEventsStream(
                app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME)))
            .filter(e -> e.getId().equals("scsmbstgra"))
            .findAny()
            .get()
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void getOutputEventsStreamFilter2() throws IOException {
    String expected = "id=scsmbstgrb, duration=3, type=null, host=null, alert=false";
    String actual =
        app.getOutputEventsStream(
                app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME)))
            .filter(e -> e.getId().equals("scsmbstgrb"))
            .findAny()
            .get()
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void getOutputEventsStreamFilter3() throws IOException {
    String expected = "id=scsmbstgrc, duration=8, type=null, host=null, alert=true";
    String actual =
        app.getOutputEventsStream(
                app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME)))
            .filter(e -> e.getId().equals("scsmbstgrc"))
            .findAny()
            .get()
            .toString();
    assertEquals(expected, actual);
  }

  @Test
  public void getOutputEventsStreamCount() throws IOException {
    long expected = 3;
    long actual =
        app.getOutputEventsStream(
                app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME)))
            .count();
    assertEquals(expected, actual);
  }

  @Test
  public void processOutputEventStream() throws IOException, SQLException {
    File outputFile = new File(getFullFileName(OUTPUT_FILE_NAME));
    if (outputFile.exists()) {
      outputFile.delete();
    }

    File etalonOutputFile = new File(getFullFileName(ETALON_OUTPUT_FILE_NAME));

    app.processOutputEventStream(
        app.getOutputEventsStream(
            app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME))),
        WORK_DB_URI,
        OUTPUT_FILE_NAME);

    assertEquals(
        Files.readAllLines(etalonOutputFile.toPath()), Files.readAllLines(outputFile.toPath()));
  }

  @Test
  @Ignore
  public void performanceTest() throws IOException, SQLException {
    File outputFile = new File(System.getProperty("user.dir") + File.separator + OUTPUT_FILE_NAME);
    if (outputFile.exists()) {
      outputFile.delete();
    }

    File etalonOutputFile =
        new File(System.getProperty("user.dir") + File.separator + ETALON_OUTPUT_FILE_NAME);

    app.processOutputEventStream(
        app.getOutputEventsStream(
            app.getInputEventStream(app.getInputStringStream(INPUT_FILE_NAME))),
        WORK_DB_URI,
        OUTPUT_FILE_NAME);

    assertEquals(
        Files.readAllLines(etalonOutputFile.toPath()), Files.readAllLines(outputFile.toPath()));
  }

  public String getFullFileName(String fileName) {
    return String.format(
        "%s%s%s",
        System.getProperty("user.dir"), File.separator, fileName.replaceAll("/", File.separator));
  }

  @Test
  @Ignore
  public void generateBigLogFile() throws IOException {
    File bigTemplateFile = new File(getFullFileName(BIG_TEMPLATE_FILE_NAME));
    File bigInputFile = new File(getFullFileName(BIG_INPUT_FILE_NAME));
    if (bigInputFile.exists()) {
      bigInputFile.delete();
    }

    List<String> template = Files.readAllLines(bigTemplateFile.toPath());

    BufferedWriter bufferedWriter =
        Files.newBufferedWriter(
            bigInputFile.toPath(),
            Charset.forName("US-ASCII"),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND);
    // E5 23 sec
    // 1E5 -> 5M rows -> 400MB

    for (int i = 1; i <= 1E5; i++) {

      int finalI = i;

      bufferedWriter.append(
          String.join(
              System.lineSeparator(),
              template
                  .stream()
                  .map(
                      s ->
                          s.replaceAll("#ID1#", String.format("id_a1_%s", finalI))
                              .replaceAll("#ID2#", String.format("id_b1_%s", finalI))
                              .replaceAll("#ID3#", String.format("id_c1_%s", finalI))
                              .replaceAll("#ID4#", String.format("id_d1_%s", finalI))
                              .replaceAll("#ID5#", String.format("id_e1_%s", finalI))
                              .replaceAll("#ID6#", String.format("id_f1_%s", finalI))
                              .replaceAll("#ID7#", String.format("id_g1_%s", finalI))
                              .replaceAll("#ID8#", String.format("id_h1_%s", finalI))
                              .replaceAll("#ID9#", String.format("id_j1_%s", finalI)))
                  .collect(Collectors.toList())));

      bufferedWriter.append(
          String.join(
              System.lineSeparator(),
              template
                  .stream()
                  .map(
                      s ->
                          s.replaceAll("#ID1#", String.format("id_a2_%s", finalI))
                              .replaceAll("#ID2#", String.format("id_b2_%s", finalI))
                              .replaceAll("#ID3#", String.format("id_c2_%s", finalI))
                              .replaceAll("#ID4#", String.format("id_d2_%s", finalI))
                              .replaceAll("#ID5#", String.format("id_e2_%s", finalI))
                              .replaceAll("#ID6#", String.format("id_f2_%s", finalI))
                              .replaceAll("#ID7#", String.format("id_g2_%s", finalI))
                              .replaceAll("#ID8#", String.format("id_h2_%s", finalI))
                              .replaceAll("#ID9#", String.format("id_j2_%s", finalI)))
                  .collect(Collectors.toList())));

      bufferedWriter.append(
          String.join(
              System.lineSeparator(),
              template
                  .stream()
                  .map(
                      s ->
                          s.replaceAll("#ID1#", String.format("id_a3_%s", finalI))
                              .replaceAll("#ID2#", String.format("id_b3_%s", finalI))
                              .replaceAll("#ID3#", String.format("id_c3_%s", finalI))
                              .replaceAll("#ID4#", String.format("id_d3_%s", finalI))
                              .replaceAll("#ID5#", String.format("id_e3_%s", finalI))
                              .replaceAll("#ID6#", String.format("id_f3_%s", finalI))
                              .replaceAll("#ID7#", String.format("id_g3_%s", finalI))
                              .replaceAll("#ID8#", String.format("id_h3_%s", finalI))
                              .replaceAll("#ID9#", String.format("id_j3_%s", finalI)))
                  .collect(Collectors.toList())));

      /*Files.write(bigInputFile.toPath(), template.stream().map(
              s -> s.replaceAll("#ID1#", String.format("id_a_%s", finalI)).
                      replaceAll("#ID2#", String.format("id_b_%s", finalI)).
                      replaceAll("#ID3#", String.format("id_c_%s", finalI))
      ).collect(Collectors.toList()), StandardOpenOption.APPEND , StandardOpenOption.CREATE);*/
    }
    bufferedWriter.close();
  }

  @Test
  public void run() {}
}
