package com.scalar.kelpie.lamb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
  private static final String TYPE = "type";
  private static final String PATTERN = "pattern";
  private static final String LIST = "list";
  private static final String LENGTH = "length";
  private static final String START = "start";
  private static final String END = "end";

  private static final String SEQUENTIAL_PATTERN = "SEQUENTIAL";
  private static final String RANDOM_PATTERN = "RANDOM";
  private static final String TIMESTAMP_PATTERN = "TIMESTAMP";

  private static final String INT_TYPE = "INT";
  private static final String BIGINT_TYPE = "BIGINT";
  private static final String DOUBLE_TYPE = "DOUBLE";
  private static final String BOOLEAN_TYPE = "BOOLEAN";
  private static final String STRING_TYPE = "STRING";

  private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789";

  private final JsonObject baseArguments;
  private final JsonObject variableConfig;
  private final Map<String, AtomicInteger> stateCounters = new ConcurrentHashMap<>();

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
        return makeValue(stringValue.substring(1, stringValue.length() - 1));
      }
    }

    return baseValue;
  }

  private JsonValue makeValue(String name) {
    JsonObject config = variableConfig.getJsonObject(name);
    String type = config.getString(TYPE);
    switch (type) {
      case INT_TYPE:
      case BIGINT_TYPE:
      case DOUBLE_TYPE:
        return makeNumberValue(name, config);
      case BOOLEAN_TYPE:
        return makeBooleanValue(name, config);
      case STRING_TYPE:
        return makeStringValue(name, config);
      default:
        throw new IllegalArgumentException(type + " is not supported for " + name);
    }
  }

  private JsonValue makeBooleanValue(String name, JsonObject config) {
    String pattern = config.getString(PATTERN);
    switch (pattern) {
      case SEQUENTIAL_PATTERN:
        return makeSequentialBoolean(name, config);
      case RANDOM_PATTERN:
        return makeRandomBoolean(name, config);
      default:
        throw new IllegalArgumentException(
            pattern + " is not supported for a boolean variable for " + name);
    }
  }

  private JsonValue makeNumberValue(String name, JsonObject config) {
    String pattern = config.getString(PATTERN);
    switch (pattern) {
      case SEQUENTIAL_PATTERN:
        return makeSequentialNumber(name, config);
      case RANDOM_PATTERN:
        return makeRandomNumber(name, config);
      case TIMESTAMP_PATTERN:
        return makeTimestamp(name, config);
      default:
        throw new IllegalArgumentException(
            pattern + " is not supported for a number variable for " + name);
    }
  }

  private JsonString makeStringValue(String name, JsonObject config) {
    String pattern = config.getString(PATTERN);
    switch (pattern) {
      case SEQUENTIAL_PATTERN:
        return makeSequentialString(name, config);
      case RANDOM_PATTERN:
        return makeRandomString(name, config);
      default:
        throw new IllegalArgumentException(
            pattern + " is not supported for a number variable for " + name);
    }
  }

  private JsonValue makeSequentialBoolean(String name, JsonObject config) {
    if (!config.containsKey(LIST)) {
      throw new IllegalArgumentException(
          "Sequential boolean variable requires a `list` for " + name);
    }

    JsonArray list = config.getJsonArray(LIST);
    int index = getAndIncrementStateCounter(name) % list.size();

    return list.getBoolean(index) ? JsonValue.TRUE : JsonValue.FALSE;
  }

  private JsonValue makeRandomBoolean(String name, JsonObject config) {
    return ThreadLocalRandom.current().nextBoolean() ? JsonValue.TRUE : JsonValue.FALSE;
  }

  private JsonNumber makeSequentialNumber(String name, JsonObject config) {
    if (config.containsKey(LIST)) {
      JsonArray list = config.getJsonArray(LIST);
      int index = getAndIncrementStateCounter(name) % list.size();

      return list.getJsonNumber(index);
    }

    if (!config.containsKey(START) || !config.containsKey(END)) {
      throw new IllegalArgumentException(
          "Sequential number variable requires a `list` or a pair of `start` and `end` for "
              + name);
    }

    String type = config.getString(TYPE);
    switch (type) {
      case INT_TYPE:
        return Json.createValue(makeSequentialInt(name, config));
      case BIGINT_TYPE:
        return Json.createValue(makeSequentialBigint(name, config));
      default:
        throw new IllegalArgumentException(
            type + " is not supported for the number with the range for " + name);
    }
  }

  private JsonNumber makeRandomNumber(String name, JsonObject config) {
    if (config.containsKey(LIST)) {
      JsonArray list = config.getJsonArray(LIST);
      int index = ThreadLocalRandom.current().nextInt(list.size());

      return list.getJsonNumber(index);
    }

    if (!config.containsKey(START) || !config.containsKey(END)) {
      throw new IllegalArgumentException(
          "Sequential number variable requires a `list` or a pair of `start` and `end` for "
              + name);
    }

    String type = config.getString(TYPE);
    switch (type) {
      case INT_TYPE:
        return Json.createValue(makeRandomInt(name, config));
      case BIGINT_TYPE:
        return Json.createValue(makeRandomBigint(name, config));
      case DOUBLE_TYPE:
        return Json.createValue(makeRandomDouble(name, config));
      default:
        throw new IllegalArgumentException(type + " is non-supported type for " + name);
    }
  }

  private int makeSequentialInt(String name, JsonObject config) {
    int start = config.getInt(START);
    int end = config.getInt(END);
    int count = end > start ? end - start : start - end;
    int value;
    if (end > start) {
      value = start + getAndIncrementStateCounter(name) % count;
    } else {
      value = start - getAndIncrementStateCounter(name) % count;
    }

    return value;
  }

  private long makeSequentialBigint(String name, JsonObject config) {
    long start = config.getJsonNumber(START).longValue();
    long end = config.getJsonNumber(END).longValue();
    long count = end > start ? end - start : start - end;
    long value;
    if (end > start) {
      value = start + getAndIncrementStateCounter(name) % count;
    } else {
      value = start - getAndIncrementStateCounter(name) % count;
    }

    return value;
  }

  private int makeRandomInt(String name, JsonObject config) {
    int start = config.getInt(START);
    int end = config.getInt(END);

    return ThreadLocalRandom.current().nextInt(start, end);
  }

  private long makeRandomBigint(String name, JsonObject config) {
    long start = config.getJsonNumber(START).longValue();
    long end = config.getJsonNumber(END).longValue();

    return ThreadLocalRandom.current().nextLong(start, end);
  }

  private double makeRandomDouble(String name, JsonObject config) {
    double start = config.getJsonNumber(START).doubleValue();
    double end = config.getJsonNumber(END).doubleValue();

    return ThreadLocalRandom.current().nextDouble(start, end);
  }

  private JsonNumber makeTimestamp(String name, JsonObject config) {
    String type = config.getString(TYPE);
    if (!type.equals(BIGINT_TYPE)) {
      throw new IllegalArgumentException("The type of TIMESTAMP should be BIGINT for " + name);
    }

    return Json.createValue(System.currentTimeMillis());
  }

  private JsonString makeSequentialString(String name, JsonObject config) {
    if (config.containsKey(LIST)) {
      JsonArray list = config.getJsonArray(LIST);
      int index = getAndIncrementStateCounter(name) % list.size();

      return list.getJsonString(index);
    }

    if (!config.containsKey(START) || !config.containsKey(END)) {
      throw new IllegalArgumentException(
          "Sequential string variable requires a `list` or a pair of `start` and `end` for "
              + name);
    }

    int value = makeSequentialInt(name, config);

    return Json.createValue(String.valueOf(value));
  }

  private JsonString makeRandomString(String name, JsonObject config) {
    if (config.containsKey(LIST)) {
      JsonArray list = config.getJsonArray(LIST);
      int index = ThreadLocalRandom.current().nextInt(list.size());

      return list.getJsonString(index);
    }

    if (config.containsKey(START) && config.containsKey(END)) {
      long start = config.getJsonNumber(START).longValue();
      long end = config.getJsonNumber(END).longValue();
      long value = ThreadLocalRandom.current().nextLong(start, end);

      return Json.createValue(String.valueOf(value));
    }

    if (!config.containsKey(LENGTH)) {
      throw new IllegalArgumentException(
          "Random string variable requires `length` of the string for " + name);
    }

    int length = config.getInt(LENGTH);
    if (length <= 0) {
      throw new IllegalArgumentException(
          "The length of random string should be positive for " + name);
    }

    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      int index = ThreadLocalRandom.current().nextInt(CHARACTERS.length());
      builder.append(CHARACTERS.charAt(index));
    }

    return Json.createValue(builder.toString());
  }

  private int getAndIncrementStateCounter(String name) {
    if (!stateCounters.containsKey(name)) {
      stateCounters.put(name, new AtomicInteger(0));
    }

    return stateCounters.get(name).getAndIncrement();
  }
}
