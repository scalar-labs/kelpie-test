package com.scalar.kelpie.lamb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.junit.Before;
import org.junit.Test;

public class ArgumentBuilderTest {
  private JsonObject baseArguments;

  private static final String CONSTANT_ARGUMENT_0 = "constant0";
  private static final String CONSTANT_ARGUMENT_1 = "constant1";
  private static final String CONSTANT_ARGUMENT_2 = "constant2";
  private static final String CONSTANT_ARGUMENT_3 = "constant1";
  private static final String CONSTANT_ARGUMENT_4 = "constant2";
  private static final String ANY_STRING_0 = "test0";
  private static final String ANY_STRING_1 = "test1";
  private static final String ANY_STRING_2 = "test2";
  private static final String ANY_STRING_3 = "test2";
  private static final int ANY_INT_0 = 0;
  private static final int ANY_INT_1 = 111;
  private static final int ANY_INT_2 = 222;
  private static final long ANY_BIGINT_0 = (long) Integer.MAX_VALUE + 1L;
  private static final long ANY_BIGINT_1 = (long) Integer.MAX_VALUE + 1234L;
  private static final long ANY_BIGINT_2 = (long) Integer.MIN_VALUE - 1L;
  private static final double ANY_DOUBLE_0 = 0.1;
  private static final double ANY_DOUBLE_1 = 2.2;
  private static final double ANY_DOUBLE_2 = 3.3;
  private static final String ARGUMENT_0 = "arg0";
  private static final String ARGUMENT_1 = "arg1";
  private static final String ARGUMENT_2 = "arg2";
  private static final String ARGUMENT_3 = "arg3";
  private static final String ARGUMENT_4 = "arg4";
  private static final String ARGUMENT_5 = "arg5";

  @Before
  public void setUp() throws Exception {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    builder.add(CONSTANT_ARGUMENT_0, ANY_STRING_0);

    JsonArrayBuilder innerArray = Json.createArrayBuilder();
    innerArray.add(ANY_STRING_1);
    innerArray.add(ANY_STRING_2);

    JsonObjectBuilder innerObject = Json.createObjectBuilder();
    innerObject.add(CONSTANT_ARGUMENT_3, ANY_INT_1);
    innerObject.add(CONSTANT_ARGUMENT_4, ANY_INT_2);

    builder.add(CONSTANT_ARGUMENT_1, innerArray.build());
    builder.add(CONSTANT_ARGUMENT_2, innerObject.build());

    baseArguments = builder.build();
  }

  @Test
  public void build_onlyConstantValuesGiven_shouldBuildSameJsonObject() {
    // Arrange
    JsonObject variableConfig = JsonObject.EMPTY_JSON_OBJECT;
    ArgumentBuilder builder = new ArgumentBuilder(baseArguments, variableConfig);

    // Act
    JsonObject actual = builder.build();

    // Assert
    assertThat(actual).isEqualTo(baseArguments);
  }

  @Test
  public void build_sequentialIntWithRangeGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "INT");
    argumentConfig.add("pattern", "SEQUENTIAL");
    argumentConfig.add("start", 0);
    argumentConfig.add("end", 5);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    builder.build();
    builder.build();
    JsonObject actual4 = builder.build();
    JsonObject actual5 = builder.build();

    // Assert
    assertThat(actual0.getInt(ARGUMENT_0)).isEqualTo(0);
    assertThat(actual1.getInt(ARGUMENT_0)).isEqualTo(1);
    assertThat(actual4.getInt(ARGUMENT_0)).isEqualTo(4);
    assertThat(actual5.getInt(ARGUMENT_0)).isEqualTo(0);
  }

  @Test
  public void build_sequentialIntWithListGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "INT");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(ANY_INT_0);
    list.add(ANY_INT_1);
    list.add(ANY_INT_2);
    argumentConfig.add("list", list.build());
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();
    JsonObject actual3 = builder.build();

    // Assert
    assertThat(actual0.getInt(ARGUMENT_0)).isEqualTo(ANY_INT_0);
    assertThat(actual1.getInt(ARGUMENT_0)).isEqualTo(ANY_INT_1);
    assertThat(actual2.getInt(ARGUMENT_0)).isEqualTo(ANY_INT_2);
    assertThat(actual3.getInt(ARGUMENT_0)).isEqualTo(ANY_INT_0);
  }

  @Test
  public void build_randomIntWithRangeGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "INT");
    argumentConfig.add("pattern", "RANDOM");
    argumentConfig.add("start", -10);
    argumentConfig.add("end", 10);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();

    // Assert
    assertThat(actual0.getInt(ARGUMENT_0)).isGreaterThanOrEqualTo(-10);
    assertThat(actual0.getInt(ARGUMENT_0)).isLessThan(10);
    assertThat(actual1.getInt(ARGUMENT_0)).isGreaterThanOrEqualTo(-10);
    assertThat(actual1.getInt(ARGUMENT_0)).isLessThan(10);
  }

  @Test
  public void build_randomIntWithListGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "INT");
    argumentConfig.add("pattern", "RANDOM");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(ANY_INT_0);
    list.add(ANY_INT_1);
    list.add(ANY_INT_2);
    argumentConfig.add("list", list.build());
    List<Integer> expected = Arrays.asList(ANY_INT_0, ANY_INT_1, ANY_INT_2);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();

    // Assert
    assertThat(actual0.getInt(ARGUMENT_0)).isIn(expected);
    assertThat(actual1.getInt(ARGUMENT_0)).isIn(expected);
    assertThat(actual2.getInt(ARGUMENT_0)).isIn(expected);
  }

  @Test
  public void build_sequentialBigintWithRangeGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    long start = ANY_BIGINT_0;
    long end = ANY_BIGINT_0 + 3L;
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "BIGINT");
    argumentConfig.add("pattern", "SEQUENTIAL");
    argumentConfig.add("start", start);
    argumentConfig.add("end", end);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();
    JsonObject actual3 = builder.build();

    // Assert
    assertThat(actual0.getJsonNumber(ARGUMENT_0).longValue()).isEqualTo(start);
    assertThat(actual1.getJsonNumber(ARGUMENT_0).longValue()).isEqualTo(start + 1);
    assertThat(actual2.getJsonNumber(ARGUMENT_0).longValue()).isEqualTo(start + 2);
    assertThat(actual3.getJsonNumber(ARGUMENT_0).longValue()).isEqualTo(start);
  }

  @Test
  public void build_sequentialBigintWithListGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "BIGINT");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(ANY_BIGINT_0);
    list.add(ANY_BIGINT_1);
    list.add(ANY_BIGINT_2);
    argumentConfig.add("list", list.build());
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();
    JsonObject actual3 = builder.build();

    // Assert
    assertThat(actual0.getJsonNumber(ARGUMENT_0).longValue()).isEqualTo(ANY_BIGINT_0);
    assertThat(actual1.getJsonNumber(ARGUMENT_0).longValue()).isEqualTo(ANY_BIGINT_1);
    assertThat(actual2.getJsonNumber(ARGUMENT_0).longValue()).isEqualTo(ANY_BIGINT_2);
    assertThat(actual3.getJsonNumber(ARGUMENT_0).longValue()).isEqualTo(ANY_BIGINT_0);
  }

  @Test
  public void build_randomBigintWithRangeGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    long start = ANY_BIGINT_0;
    long end = ANY_BIGINT_0 + 10L;
    argumentConfig.add("type", "BIGINT");
    argumentConfig.add("pattern", "RANDOM");
    argumentConfig.add("start", start);
    argumentConfig.add("end", end);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();

    // Assert
    assertThat(actual0.getJsonNumber(ARGUMENT_0).longValue()).isGreaterThanOrEqualTo(start);
    assertThat(actual0.getJsonNumber(ARGUMENT_0).longValue()).isLessThan(end);
    assertThat(actual1.getJsonNumber(ARGUMENT_0).longValue()).isGreaterThanOrEqualTo(start);
    assertThat(actual1.getJsonNumber(ARGUMENT_0).longValue()).isLessThan(end);
  }

  @Test
  public void build_randomBigintWithListGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "BIGINT");
    argumentConfig.add("pattern", "RANDOM");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(ANY_BIGINT_0);
    list.add(ANY_BIGINT_1);
    list.add(ANY_BIGINT_2);
    argumentConfig.add("list", list.build());
    List<Long> expected = Arrays.asList(ANY_BIGINT_0, ANY_BIGINT_1, ANY_BIGINT_2);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();

    // Assert
    assertThat(actual0.getJsonNumber(ARGUMENT_0).longValue()).isIn(expected);
    assertThat(actual1.getJsonNumber(ARGUMENT_0).longValue()).isIn(expected);
    assertThat(actual2.getJsonNumber(ARGUMENT_0).longValue()).isIn(expected);
  }

  @Test
  public void build_sequentialDoubleWithListGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "DOUBLE");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(ANY_DOUBLE_0);
    list.add(ANY_DOUBLE_1);
    list.add(ANY_DOUBLE_2);
    argumentConfig.add("list", list.build());
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();
    JsonObject actual3 = builder.build();

    // Assert
    assertThat(actual0.getJsonNumber(ARGUMENT_0).doubleValue()).isEqualTo(ANY_DOUBLE_0);
    assertThat(actual1.getJsonNumber(ARGUMENT_0).doubleValue()).isEqualTo(ANY_DOUBLE_1);
    assertThat(actual2.getJsonNumber(ARGUMENT_0).doubleValue()).isEqualTo(ANY_DOUBLE_2);
    assertThat(actual3.getJsonNumber(ARGUMENT_0).doubleValue()).isEqualTo(ANY_DOUBLE_0);
  }

  @Test
  public void build_timestampBigintGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "BIGINT");
    argumentConfig.add("pattern", "TIMESTAMP");
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    JsonObject actual1 = builder.build();

    // Assert
    long diff =
        actual1.getJsonNumber(ARGUMENT_0).longValue()
            - actual0.getJsonNumber(ARGUMENT_0).longValue();
    assertThat(diff).isGreaterThanOrEqualTo(1000);
  }

  @Test
  public void build_sequentialNumberWithoutListAndRangeGiven_shouldThrowIllegalArgumentException() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "INT");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act assert
    assertThatThrownBy(
            () -> {
              builder.build();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void build_sequentialNumberWithoutEndGiven_shouldThrowIllegalArgumentException() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "INT");
    argumentConfig.add("pattern", "SEQUENTIAL");
    argumentConfig.add("start", ANY_INT_1);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act assert
    assertThatThrownBy(
            () -> {
              builder.build();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void build_timestampNotBigIntGiven_shouldThrowIllegalArgumentException() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "INT");
    argumentConfig.add("pattern", "TIMESTAMP");
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act assert
    assertThatThrownBy(
            () -> {
              builder.build();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void build_nonSupportedTypeGiven_shouldThrowIllegalArgumentException() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "FLOAT");
    argumentConfig.add("pattern", "SEQUENTIAL");
    argumentConfig.add("start", ANY_INT_1);
    argumentConfig.add("end", ANY_INT_2);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act assert
    assertThatThrownBy(
            () -> {
              builder.build();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void build_sequentialBooleanWithListGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "BOOLEAN");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(true);
    list.add(false);
    list.add(false);
    argumentConfig.add("list", list.build());
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();
    JsonObject actual3 = builder.build();

    // Assert
    assertThat(actual0.getBoolean(ARGUMENT_0)).isTrue();
    assertThat(actual1.getBoolean(ARGUMENT_0)).isFalse();
    assertThat(actual2.getBoolean(ARGUMENT_0)).isFalse();
    assertThat(actual3.getBoolean(ARGUMENT_0)).isTrue();
  }

  @Test
  public void build_randomBooleanGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "BOOLEAN");
    argumentConfig.add("pattern", "RANDOM");
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual = builder.build();

    // Assert
    assertThat(actual.getBoolean(ARGUMENT_0)).isNotNull();
  }

  @Test
  public void build_sequentialBooleanWithoutListGiven_shouldThrowIllegalArgumentException() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "BOOLEAN");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act assert
    assertThatThrownBy(
            () -> {
              builder.build();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void build_sequentialStringWithListGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "STRING");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(ANY_STRING_0);
    list.add(ANY_STRING_1);
    list.add(ANY_STRING_2);
    argumentConfig.add("list", list.build());
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();
    JsonObject actual3 = builder.build();

    // Assert
    assertThat(actual0.getString(ARGUMENT_0)).isEqualTo(ANY_STRING_0);
    assertThat(actual1.getString(ARGUMENT_0)).isEqualTo(ANY_STRING_1);
    assertThat(actual2.getString(ARGUMENT_0)).isEqualTo(ANY_STRING_2);
    assertThat(actual3.getString(ARGUMENT_0)).isEqualTo(ANY_STRING_0);
  }

  @Test
  public void build_randomStringWithListGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "STRING");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(ANY_STRING_0);
    list.add(ANY_STRING_1);
    list.add(ANY_STRING_2);
    argumentConfig.add("list", list.build());
    List<String> expected = Arrays.asList(ANY_STRING_0, ANY_STRING_1, ANY_STRING_2);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();

    // Assert
    assertThat(actual0.getString(ARGUMENT_0)).isIn(expected);
    assertThat(actual1.getString(ARGUMENT_0)).isIn(expected);
    assertThat(actual2.getString(ARGUMENT_0)).isIn(expected);
  }

  @Test
  public void build_sequentialStringWithRangeGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "STRING");
    argumentConfig.add("pattern", "SEQUENTIAL");
    argumentConfig.add("start", 0);
    argumentConfig.add("end", 3);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();
    JsonObject actual3 = builder.build();

    // Assert
    assertThat(actual0.getString(ARGUMENT_0)).isEqualTo("0");
    assertThat(actual1.getString(ARGUMENT_0)).isEqualTo("1");
    assertThat(actual2.getString(ARGUMENT_0)).isEqualTo("2");
    assertThat(actual3.getString(ARGUMENT_0)).isEqualTo("0");
  }

  @Test
  public void build_randomStringWithLengthGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "STRING");
    argumentConfig.add("pattern", "RANDOM");
    argumentConfig.add("length", ANY_INT_1);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual = builder.build();

    // Assert
    assertThat(actual.getString(ARGUMENT_0).length()).isEqualTo(ANY_INT_1);
  }

  @Test
  public void build_randomStringWithRangeGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "STRING");
    argumentConfig.add("pattern", "RANDOM");
    argumentConfig.add("start", ANY_BIGINT_0);
    argumentConfig.add("end", ANY_BIGINT_1);
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();

    // Assert
    long value0 = Long.valueOf(actual0.getString(ARGUMENT_0));
    assertThat(value0).isGreaterThanOrEqualTo(ANY_BIGINT_0);
    assertThat(value0).isLessThan(ANY_BIGINT_1);
    long value1 = Long.valueOf(actual1.getString(ARGUMENT_0));
    assertThat(value1).isGreaterThanOrEqualTo(ANY_BIGINT_0);
    assertThat(value1).isLessThan(ANY_BIGINT_1);
  }

  @Test
  public void build_sequentialStringWithoutListGiven_shouldThrowIllegalArgumentException() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "STRING");
    argumentConfig.add("pattern", "SEQUENTIAL");
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act assert
    assertThatThrownBy(
            () -> {
              builder.build();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void build_randomStringWithoutListAndLengthGiven_shouldThrowIllegalArgumentException() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    baseBuilder.add(ARGUMENT_0, "_" + ARGUMENT_0 + "_");
    JsonObjectBuilder argumentConfig = Json.createObjectBuilder();
    argumentConfig.add("type", "STRING");
    argumentConfig.add("pattern", "RANDOM");
    JsonObjectBuilder target = Json.createObjectBuilder();
    target.add(ARGUMENT_0, argumentConfig);
    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act assert
    assertThatThrownBy(
            () -> {
              builder.build();
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void build_arrayVariablesGiven_shouldBuildProperty() {
    // Arrange
    JsonObjectBuilder baseBuilder = Json.createObjectBuilder(baseArguments);
    JsonArrayBuilder innerArray = Json.createArrayBuilder();
    innerArray.add("_" + ARGUMENT_1 + "_");
    innerArray.add("_" + ARGUMENT_2 + "_");
    baseBuilder.add(ARGUMENT_0, innerArray.build());

    JsonObjectBuilder target = Json.createObjectBuilder();
    // arg1
    JsonObjectBuilder argumentConfig1 = Json.createObjectBuilder();
    argumentConfig1.add("type", "INT");
    argumentConfig1.add("pattern", "SEQUENTIAL");
    JsonArrayBuilder list = Json.createArrayBuilder();
    list.add(ANY_INT_0);
    list.add(ANY_INT_1);
    argumentConfig1.add("list", list.build());
    target.add(ARGUMENT_1, argumentConfig1);
    // arg2
    JsonObjectBuilder argumentConfig2 = Json.createObjectBuilder();
    argumentConfig2.add("type", "INT");
    argumentConfig2.add("pattern", "SEQUENTIAL");
    argumentConfig2.add("start", 10);
    argumentConfig2.add("end", 15);
    target.add(ARGUMENT_2, argumentConfig2);

    ArgumentBuilder builder = new ArgumentBuilder(baseBuilder.build(), target.build());

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    JsonObject actual2 = builder.build();
    JsonObject actual3 = builder.build();
    JsonObject actual4 = builder.build();
    JsonObject actual5 = builder.build();

    // Assert
    assertThat(actual0.getJsonArray(ARGUMENT_0).getInt(0)).isEqualTo(ANY_INT_0);
    assertThat(actual0.getJsonArray(ARGUMENT_0).getInt(1)).isEqualTo(10);
    assertThat(actual1.getJsonArray(ARGUMENT_0).getInt(0)).isEqualTo(ANY_INT_1);
    assertThat(actual1.getJsonArray(ARGUMENT_0).getInt(1)).isEqualTo(11);
    assertThat(actual2.getJsonArray(ARGUMENT_0).getInt(0)).isEqualTo(ANY_INT_0);
    assertThat(actual2.getJsonArray(ARGUMENT_0).getInt(1)).isEqualTo(12);
    assertThat(actual3.getJsonArray(ARGUMENT_0).getInt(0)).isEqualTo(ANY_INT_1);
    assertThat(actual3.getJsonArray(ARGUMENT_0).getInt(1)).isEqualTo(13);
    assertThat(actual4.getJsonArray(ARGUMENT_0).getInt(0)).isEqualTo(ANY_INT_0);
    assertThat(actual4.getJsonArray(ARGUMENT_0).getInt(1)).isEqualTo(14);
    assertThat(actual5.getJsonArray(ARGUMENT_0).getInt(0)).isEqualTo(ANY_INT_1);
    assertThat(actual5.getJsonArray(ARGUMENT_0).getInt(1)).isEqualTo(10);
  }
}
