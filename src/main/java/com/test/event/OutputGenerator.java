package com.test.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * {@code OutputGenerator} class responsible to convert stream of {@code OutputEvent} into output
 * file with processed events using HSQLDB
 */
public class OutputGenerator {
  private static final Logger logger = LoggerFactory.getLogger(OutputGenerator.class);

  private static final String WORK_TABLE_NAME = "w_csa_tmp";
  private static final String CREATE_WORK_TABLE_SQL =
      "create text table if not exists %s (id varchar(4000), duration numeric, type varchar(4000), host varchar(4000), alert boolean)";
  private static final String INSERT_WORK_TABLE_SQL = "insert into %s values (?, ?, ?, ?, ?)";
  private static final String SET_TABLE_SOURCE_SQL = "set table %s source \"%s\"";
  public static String WORKING_DATABASE_URI;
  public static String OUTPUT_FILE_NAME;

  private Connection conn;
  private PreparedStatement preparedStatement = null;

  private long emitCount = 0;

  /**
   * Creates a new instance of HSQLDB engine to produce output file.
   *
   * @param workingDatabaseURI HSQLDB URI
   * @param outputFileName putput file
   * @throws SQLException
   */
  public OutputGenerator(String workingDatabaseURI, String outputFileName) throws SQLException {
    initConnection(workingDatabaseURI, outputFileName);
  }

  private void initConnection(String workingDatabaseURI, String outputFileName)
      throws SQLException {
    logger.info("hsqldb connection initialization");
    conn = DriverManager.getConnection(workingDatabaseURI);
    Statement stmt = conn.createStatement();
    stmt.execute(String.format(CREATE_WORK_TABLE_SQL, WORK_TABLE_NAME));
    stmt.execute(String.format(SET_TABLE_SOURCE_SQL, WORK_TABLE_NAME, outputFileName));
    logger.info("hsqldb connection established");
    logger.debug("URI: {}", workingDatabaseURI);
    logger.debug("OutputFileName: {}", outputFileName);
    logger.debug("AutoCommit: {}", conn.getAutoCommit());
  }

  public void releaseConnection() throws SQLException {
    if (conn != null) {
      logger.debug("Releasing connection");
      conn.close();
      conn = null;
    } else {
      logger.debug("Releasing connection - nothing to release!");
    }
  }

  /**
   * Saves a new record into output file.
   *
   * @param outputEvent {@code OutputEvent} to be recorded
   * @throws SQLException
   */
  public void emit(OutputEvent outputEvent) throws SQLException {
    logger.debug("Emit: {}", outputEvent);
    emitOutputEvent(outputEvent);
  }

  private void emitOutputEvent(OutputEvent outputEvent) throws SQLException {
    if (preparedStatement == null) {
      logger.debug(
          "Preparing SQL-statement: {}", String.format(INSERT_WORK_TABLE_SQL, WORK_TABLE_NAME));
      preparedStatement =
          conn.prepareStatement(String.format(INSERT_WORK_TABLE_SQL, WORK_TABLE_NAME));
      logger.debug("Prepared");
    }

    preparedStatement.setString(1, outputEvent.getId());
    preparedStatement.setBigDecimal(2, outputEvent.getDuration());
    preparedStatement.setString(3, outputEvent.getType());
    preparedStatement.setString(4, outputEvent.getHost());
    preparedStatement.setBoolean(5, outputEvent.getAlert());

    logger.debug("Executing prepared statement...");
    preparedStatement.executeUpdate();
    logger.debug("... finished");
    emitCount++;
  }

  /**
   * Initializes new instance.
   *
   * @return a new {@code OutputGenerator}
   */
  public static OutputGenerator init() {
    logger.debug("init");
    try {
      return new OutputGenerator(WORKING_DATABASE_URI, OUTPUT_FILE_NAME);
    } catch (SQLException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Apply method to consume {@code OutputEvent} in stream
   *
   * @param outputEvent
   * @return
   */
  public OutputGenerator apply(OutputEvent outputEvent) {
    logger.debug("apply");
    try {
      this.emit(outputEvent);
    } catch (SQLException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }
    return this;
  }

  /**
   * Combines two partial results of {@code OutputGenerator} - just sums their counts.
   *
   * @param outputGenerator1
   * @param outputGenerator2
   * @return
   */
  public static OutputGenerator combine(
      OutputGenerator outputGenerator1, OutputGenerator outputGenerator2) {
    logger.debug("combine");
    outputGenerator1.emitCount += outputGenerator2.emitCount;
    return outputGenerator1;
  }

  /**
   * Used to finalize instance and close connection at the end of processing.
   *
   * @return the same instance
   */
  public OutputGenerator release() {
    logger.debug("release");
    try {
      releaseConnection();
    } catch (SQLException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }
    return this;
  }

  /** @return count of produced events into output file */
  public long getEmitCount() {
    return emitCount;
  }
}
