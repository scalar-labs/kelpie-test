package com.scalar.kelpie.lamb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.File;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import java.io.StringReader;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import org.junit.Before;

public class ArgumentBuilderTest {
  private JsonObject baseArguments;

  @Before
  public void setUp() throws Exception {
    baseArguments = makeJsonObject("{"
        + "\"constant1\": \"test\","
        + "\"constant2\": 222,"
        + "\"constant_list\": [\"a1\", \"a2\"]"
        + "}");
  }

  @Test
  public void build_sequentialIntGiven_shouldBuildProperty() {
    // Arrange
    String target = "{"
      + "\"n1\": {"
      + "\"type\": \"INT\","
      + "\"pattern\": \"SEQUENTIAL\","
      + "\"start\": 0,"
      + "\"end\": 5"
      + "}"
      + "}";
    JsonObject variableConfig = makeJsonObject(target);
    ArgumentBuilder builder = new ArgumentBuilder(baseArguments, variableConfig);

    // Act
    JsonObject actual0 = builder.build();
    JsonObject actual1 = builder.build();
    builder.build();
    builder.build();
    JsonObject actual4 = builder.build();
    JsonObject actual5 = builder.build();

    // Assert
    assertThat(actual0.getInt("n1")).isEqualTo(0);
    assertThat(actual1.getInt("n1")).isEqualTo(1);
    assertThat(actual4.getInt("n1")).isEqualTo(4);
    assertThat(actual5.getInt("n1")).isEqualTo(0);
  }

  private JsonObject makeJsonObject(String stringJson) {
    try (StringReader stringReader = new StringReader(stringJson);
        JsonReader reader = Json.createReader(stringReader)) {
      return reader.readObject();
    }
  }
}
