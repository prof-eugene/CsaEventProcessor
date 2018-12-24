package com.test;

import com.test.event.InputEvent;
import com.test.event.InputEventFactory;
import com.test.event.OutputEvent;
import com.test.event.OutputGenerator;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Date;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class CsaEventProcessor {
  private static final Logger logger = LoggerFactory.getLogger(CsaEventProcessor.class);

  public CsaEventProcessor() {}

  /** @return options settings for the application */
  public static Options getCmdOptions() {
    Options options = new Options();

    Option inputOption = new Option("i", "input", true, "input file name");
    inputOption.setRequired(true);
    options.addOption(inputOption);

    Option wdOption =
        new Option(
            "wd",
            "working-database-URL",
            true,
            "HSQLDB working database URI (example \"jdbc:hsqldb:file:/tmp/test1/sampledb;shutdown=true\")");
    wdOption.setRequired(true);
    options.addOption(wdOption);

    Option outputOption = new Option("o", "output", true, "output file name");
    outputOption.setRequired(true);
    options.addOption(outputOption);

    Option stOption =
        new Option(
            "st", "single-thread", false, "use single-thread processing (reduces memory load)");
    stOption.setRequired(false);
    options.addOption(stOption);

    Option alertThresholdOption =
        new Option(
            "at",
            "alert-threshold",
            true,
            "alert output events if their duration is longer (default 4 ms)");
    alertThresholdOption.setRequired(false);
    options.addOption(alertThresholdOption);

    return options;
  }

  /**
   * Converts text file into stream of strings
   *
   * @param inputFileName source file name
   * @return stream of strings
   * @throws IOException
   */
  public Stream<String> getInputStringStream(String inputFileName) throws IOException {
    return Files.lines(Paths.get(inputFileName));
  }

  /**
   * Converts stream of strings onto stream of {@code InputEvent}
   *
   * @param inputStream input stream
   * @return output stream
   */
  public Stream<InputEvent> getInputEventStream(Stream<String> inputStream) {
      return inputStream.map(InputEventFactory::buildEvent);
  }

  /**
   * Converts stream of {@code InputEvent} into stream of {@code OutputEvent}. New {@code
   * OutputEvent} represents a result of processing all {@code InputEvent} with the same id of the
   * event.
   *
   * @param inputStream
   * @return
   */
  public Stream<OutputEvent> getOutputEventsStream(Stream<InputEvent> inputStream) {
    return inputStream
        .collect(
            groupingBy(
                InputEvent::getId,
                Collector.of(OutputEvent::new, OutputEvent::accept, OutputEvent::combiner)))
        .values()
        .stream();
  }

  /**
   * Converts stream of {@code OutputEvent} into HSQLDB file.
   *
   * @param outputEventStream input stream
   * @param workingDatabaseURI HSQLDB database URI
   * @param outputFileName output file name
   * @return count of generate records in the output file
   * @throws SQLException
   */
  public long processOutputEventStream(
      Stream<OutputEvent> outputEventStream, String workingDatabaseURI, String outputFileName)
      throws SQLException {
    // long resultCount = outputEventStream.map(s -> OutputGenerator.apply(workingDatabaseURI,
    // outputFileName, s)).count();

    // long resultCount = outputEventStream.parallel().map(s ->
    // OutputGenerator.apply(workingDatabaseURI, outputFileName, s)).count();
    // OutputGenerator.releaseInstance();

    OutputGenerator.WORKING_DATABASE_URI = workingDatabaseURI;
    OutputGenerator.OUTPUT_FILE_NAME = outputFileName;
    // outputEventStream.parallel().collect(Collector.of(OutputGenerator::init,
    // OutputGenerator::apply, OutputGenerator::combine, OutputGenerator::release));
    return outputEventStream
        .collect(
            Collector.of(
                OutputGenerator::init,
                OutputGenerator::apply,
                OutputGenerator::combine,
                OutputGenerator::release))
        .getEmitCount();
  }

  /**
   * Entry point to process an input text file of {@code InputEvent} events into output file of
   * processed {@code OutputEvent} events
   *
   * @param inputFileName input file name
   * @param workingDatabaseURI HSQLDB URI
   * @param outputFileName output file name (relative path)
   * @param parallelMode run in multi-thread mode
   * @throws IOException
   * @throws SQLException
   */
  public void run(
      String inputFileName, String workingDatabaseURI, String outputFileName, boolean parallelMode)
      throws IOException, SQLException {
    logger.info("inputFileName: {}", inputFileName);
    logger.info("workingDatabaseURI: {}", workingDatabaseURI);
    logger.info("outputFileName: {}", outputFileName);
    logger.info("parallelMode: {}", parallelMode);
    Date date = new Date();
    long startTime = date.getTime();
    logger.info("{}", startTime);
    long count;
    if (parallelMode) {
      count =
          processOutputEventStream(
              getOutputEventsStream(
                      getInputEventStream(getInputStringStream(inputFileName).parallel()))
                  .parallel(),
              workingDatabaseURI,
              outputFileName);
    } else {
      count =
          processOutputEventStream(
              getOutputEventsStream(getInputEventStream(getInputStringStream(inputFileName))),
              workingDatabaseURI,
              outputFileName);
    }
    date = new Date();
    long finishTime = date.getTime() + 1;
    logger.info("{}", finishTime);
    logger.info(
        "Performance OutputEvents: {} events, {} ms, {} ms per event, {} event per ms",
        count,
        (finishTime - startTime),
        Double.valueOf(finishTime - startTime) / count,
        Double.valueOf(count) / (finishTime - startTime));
  }

  public static void main(String[] args) {
    logger.info("Starting {}", CsaEventProcessor.class.getName());

    try {
      CommandLine cmd = new DefaultParser().parse(CsaEventProcessor.getCmdOptions(), args);

      String inputFileName = cmd.getOptionValue("input");

      String workingDatabaseURI = cmd.getOptionValue("working-database-URL");

      String outputFileName = cmd.getOptionValue("output");

      boolean parallelMode = !cmd.hasOption("single-thread");

      if (cmd.hasOption("alert-threshold")) {
        OutputEvent.ALERT_THRESHOLD = Long.valueOf(cmd.getOptionValue("alert-threshold"));
      }

      /*
                  6GB RAM - 1.8M out records in parallel and fail
      6GM, 1M input-limit, 105sec,  OutputEvents: 529413 events, 0.2004635322517581 ms per event, 4.988438489371325 event per ms
      Performance Parallel OutputEvents: 1058823 events, 160930 ms, 0.15198952043920466 ms per event, 6.579400981793326 event per ms
      Performance Single-thread OutputEvents: 2700000 events, 603651 ms, 0.22357444444444444 ms per event, 4.472783114746766 event per ms
                   */

      // OutputGenerator.initInstance(workingDatabaseURI, outputFileName);
      new CsaEventProcessor().run(inputFileName, workingDatabaseURI, outputFileName, parallelMode);
      // OutputGenerator.releaseInstance();

      logger.info("Processing completed");
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      new HelpFormatter().printHelp("CsaEventProcessor", CsaEventProcessor.getCmdOptions());
      System.exit(1);
    } catch (IOException | SQLException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
