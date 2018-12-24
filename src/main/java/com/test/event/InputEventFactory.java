package com.test.event;

import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;

/**
 * Static factory to produce {@code InputEvent} from strings. The class is completely static to make
 * it parallel-processing friendly.
 */
public class InputEventFactory {
  private static final Logger logger = LoggerFactory.getLogger(InputEventFactory.class);

  /**
   * Converts single-line string JSON object into {@code InputEvent}
   *
   * @param jsonString input string
   * @return a new {@code InputEvent}
   */
  public static InputEvent buildEvent(String jsonString) {
    logger.debug("Input string: {}", jsonString);
    JsonReader reader = new JsonReader(new StringReader(jsonString));
    InputEvent event = null;
    try {
      reader.beginObject();

      String id = null;
      String state = null;
      BigDecimal timestamp = null;
      String type = null;
      String host = null;

      while (reader.hasNext()) {
        String name = reader.nextName();
        switch (name) {
          case "id":
            id = reader.nextString();
            break;
          case "state":
            state = reader.nextString();
            break;
          case "timestamp":
            timestamp = new BigDecimal(reader.nextString());
            break;
          case "type":
            type = reader.nextString();
            break;
          case "host":
            host = reader.nextString();
            break;
          default:
            reader.skipValue();
            break;
        }
      }
      reader.endObject();

      event = new InputEvent(id, state, timestamp, type, host);
    } catch (IOException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }

    logger.debug("Object: {}", event == null ? "not created" : event.toString());
    return event;
  }
}
