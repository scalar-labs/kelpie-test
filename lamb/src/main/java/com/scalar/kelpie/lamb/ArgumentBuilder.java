package com.scalar.kelpie.lamb;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;

@ThreadSafe
public class ArgumentBuilder {
  private final JsonObject baseArguments;
  private final JsonObject variableConfig;
  private final AtomicInteger stateCounter = new AtomicInteger(0);

  private static final String TYPE = "type";
  private static final String PATTERN = "pattern";
  private static final String LIST = "list";
  private static final String LENGTH = "length";
  private static final String START = "start";
  private static final String END = "end";

  private static final String SEQUENTIAL_PATTERN = "SEQUENTIAL";
  private static final String RANDOM_PATTERN = "RANDOM";
  private static final String TIMESTAMP_PATTERN = "TIMESTAMP";

  private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789";

  public ArgumentBuilder(JsonObject baseArguments, JsonObject variableConfig) {
    this.baseArguments = baseArguments;
    this.variableConfig = variableConfig;
  }

  public JsonObject build() {
    JsonObjectBuilder builder = Json.createObjectBuilder();

    for (Map.Entry<String, JsonValue> entry : baseArguments.entrySet()) {
      JsonValue value = getJsonValue(entry.getValue());
      builder.add(entry.getKey(), value);
    }

    return builder.build();
  }

  private JsonValue getJsonValue(JsonValue baseValue) {
    JsonValue.ValueType type = baseValue.getValueType();

    if (type == JsonValue.ValueType.ARRAY) {
      JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
      for (JsonValue v : ((JsonArray) baseValue)) {
        JsonValue value = getJsonValue(v);
        arrayBuilder.add(value);
      }
      return arrayBuilder.build();
    }

    if (type == JsonValue.ValueType.OBJECT) {
      JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
      for (Map.Entry<String, JsonValue> entry : ((JsonObject) baseValue).entrySet()) {
        JsonValue value = getJsonValue(entry.getValue());
        objectBuilder.add(entry.getKey(), value);
      }
      return objectBuilder.build();
    }

    if (type == JsonValue.ValueType.STRING) {
      String stringValue = ((JsonString) baseValue).getString();
      if (stringValue.startsWith("_") && stringValue.endsWith("_")) {
        // variable
        return makeValue(stringValue);
      }
    }

    return baseValue;
  }

  private JsonValue makeValue(String name) {
    JsonObject config = variableConfig.getJsonObject(name);
    String type = config.getString(TYPE);
    switch (type) {
      case "INT":
      case "BIGINT":
      case "FLOAT":
      case "DOUBLE":
        return makeNumberValue(config);
      case "BOOLEAN":
        return makeBooleanValue(config);
      case "STRING":
        return makeStringValue(config);
      default:
        throw new IllegalArgumentException(type + " is not supported for " + name);
    }
  }

  private JsonValue makeBooleanValue(JsonObject config) {
    String pattern = config.getString(PATTERN);
    switch (pattern) {
      case SEQUENTIAL_PATTERN:
        return makeSequentialBoolean(config);
      case RANDOM_PATTERN:
        return makeRandomBoolean(config);
      default:
        throw new IllegalArgumentException(pattern + " is not supported for a boolean variable");
    }
  }

  private JsonValue makeNumberValue(JsonObject config) {
    String pattern = config.getString(PATTERN);
    switch (pattern) {
      case SEQUENTIAL_PATTERN:
        return makeSequentialNumber(config);
      case RANDOM_PATTERN:
        return makeRandomNumber(config);
      case TIMESTAMP_PATTERN:
        return makeTimestamp(config);
      default:
        throw new IllegalArgumentException(pattern + " is not supported for a number variable");
    }
  }

  private JsonString makeStringValue(JsonObject config) {
    String pattern = config.getString(PATTERN);
    switch (pattern) {
      case SEQUENTIAL_PATTERN:
        return makeSequentialString(config);
      case RANDOM_PATTERN:
        return makeRandomString(config);
      default:
        throw new IllegalArgumentException(pattern + " is not supported for a number variable");
    }
  }

  private JsonValue makeSequentialBoolean(JsonObject config) {
    JsonArray list = config.getJsonArray(LIST);
    if (list == null) {
      throw new IllegalArgumentException("Sequential boolean variable requires a `list`");
    }
    int index = stateCounter.getAndIncrement() % list.size();

    return list.getBoolean(index) ? JsonValue.TRUE : JsonValue.FALSE;
  }

  private JsonValue makeRandomBoolean(JsonObject config) {
    return ThreadLocalRandom.current().nextBoolean() ? JsonValue.TRUE : JsonValue.FALSE;
  }

  private JsonNumber makeSequentialNumber(JsonObject config) {
    JsonArray list = config.getJsonArray(LIST);
    if (list != null) {
      int index = stateCounter.getAndIncrement() % list.size();
      return list.getJsonNumber(index);
    }

    int v;
    try {
      int start = config.getInt(START);
      int end = config.getInt(END);
      int count = end > start ? end - start : start - end;
      if (end > start) {
        v = start + stateCounter.getAndIncrement() % count;
      } else {
        v = start - stateCounter.getAndIncrement() % count;
      }
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(
          "Sequential number variable requires a `list` or a pair of `start` and `end`");
    }

    return Json.createValue(v);
  }

  private JsonNumber makeRandomNumber(JsonObject config) {
    JsonArray list = config.getJsonArray(LIST);
    if (list != null) {
      int index = ThreadLocalRandom.current().nextInt(list.size());
      return list.getJsonNumber(index);
    }

    try {
      String type = config.getString(TYPE);
      switch (type) {
        case "INT":
          int start = config.getInt(START);
          int end = config.getInt(END);
          int intValue = ThreadLocalRandom.current().nextInt(start, end);
          return Json.createValue(intValue);
        case "BIGINT":
          long longStart = config.getJsonNumber(START).longValue();
          long longEnd = config.getJsonNumber(END).longValue();
          long longValue = ThreadLocalRandom.current().nextLong(longStart, longEnd);
          return Json.createValue(longValue);
        case "DOUBLE":
          double doubleStart = config.getJsonNumber(START).doubleValue();
          double doubleEnd = config.getJsonNumber(END).doubleValue();
          double doubleValue = ThreadLocalRandom.current().nextDouble(doubleStart, doubleEnd);
          return Json.createValue(doubleValue);
        default:
          throw new IllegalArgumentException(type + " is not supported for the number");
      }
    } catch (NullPointerException e) {
      throw new IllegalArgumentException(
          "Sequential number variable requires a `list` or a pair of `start` and `end`");
    }
  }

  private JsonNumber makeTimestamp(JsonObject config) {
    String type = config.getString(TYPE);
    if (type != "BIGINT") {
      throw new IllegalArgumentException("The type of TIMESTAMP should be BIGINT");
    }

    return Json.createValue(System.currentTimeMillis());
  }

  private JsonString makeSequentialString(JsonObject config) {
    JsonArray list = config.getJsonArray(LIST);
    if (list == null) {
      throw new IllegalArgumentException("Sequential string variable requires a `list`");
    }
    int index = stateCounter.getAndIncrement() % list.size();

    return list.getJsonString(index);
  }

  private JsonString makeRandomString(JsonObject config) {
    JsonArray list = config.getJsonArray(LIST);
    if (list != null) {
      int index = ThreadLocalRandom.current().nextInt(list.size());
      return list.getJsonString(index);
    }

    try {
      int length = config.getInt(LENGTH);
      StringBuilder builder = new StringBuilder(length);
      for (int i = 0; i < length; i++) {
        int index = ThreadLocalRandom.current().nextInt(CHARACTERS.length());
        builder.append(CHARACTERS.charAt(index));
      }
      return Json.createValue(builder.toString());
    } catch (NullPointerException e) {
      throw new IllegalArgumentException("Random string variable requires `length` of the string");
    }
  }
}
