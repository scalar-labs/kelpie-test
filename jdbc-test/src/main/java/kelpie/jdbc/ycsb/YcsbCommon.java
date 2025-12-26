package kelpie.jdbc.ycsb;

import com.scalar.kelpie.config.Config;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class YcsbCommon {
  static final long DEFAULT_RECORD_COUNT = 1000;
  static final long DEFAULT_PAYLOAD_SIZE = 1000;
  static final String TABLE = "usertable";
  static final String YCSB_KEY = "ycsb_key";
  static final String PAYLOAD = "payload";
  static final String CONFIG_NAME = "ycsb_config";
  static final String DB_CONFIG_NAME = "database_config";
  static final String RECORD_COUNT = "record_count";
  static final String PAYLOAD_SIZE = "payload_size";
  static final String OPS_PER_TX = "ops_per_tx";
  private static final int CHAR_START = 32; // [space]
  private static final int CHAR_STOP = 126; // [~]
  private static final char[] CHAR_SYMBOLS = new char[1 + CHAR_STOP - CHAR_START];

  static {
    for (int i = 0; i < CHAR_SYMBOLS.length; i++) {
      CHAR_SYMBOLS[i] = (char) (CHAR_START + i);
    }
  }

  private static final int[] FAST_MASKS = {
    554189328, // 10000
    277094664, // 01000
    138547332, // 00100
    69273666, // 00010
    34636833, // 00001
    346368330, // 01010
    727373493, // 10101
    588826161, // 10001
    935194491, // 11011
    658099827, // 10011
  };

  public static int getRecordCount(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, RECORD_COUNT, DEFAULT_RECORD_COUNT);
  }

  public static int getPayloadSize(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, PAYLOAD_SIZE, DEFAULT_PAYLOAD_SIZE);
  }

  // This method is taken from benchbase.
  // https://github.com/cmu-db/benchbase/blob/bbe8c1db84ec81c6cdec6fbeca27b24b1b4e6612/src/main/java/com/oltpbenchmark/util/TextGenerator.java#L80
  public static char[] randomFastChars(Random rng, char[] chars) {
    // Ok so now the goal of this is to reduce the number of times that we have to
    // invoke a random number. We'll do this by grabbing a single random int
    // and then taking different bitmasks

    int num_rounds = chars.length / FAST_MASKS.length;
    int i = 0;
    for (int ctr = 0; ctr < num_rounds; ctr++) {
      int rand = rng.nextInt(10000); // CHAR_SYMBOLS.length);
      for (int mask : FAST_MASKS) {
        chars[i++] = CHAR_SYMBOLS[(rand | mask) % CHAR_SYMBOLS.length];
      }
    }
    // Use the old way for the remaining characters
    // I am doing this because I am too lazy to think of something more clever
    for (; i < chars.length; i++) {
      chars[i] = CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
    }
    return (chars);
  }

  public static List<String> read(Connection connection, List<Integer> userIds)
      throws SQLException {
    if (userIds.isEmpty()) {
      return new ArrayList<>();
    }

    String placeholders = userIds.stream().map(id -> "?").collect(Collectors.joining(", "));
    String sql =
        "select "
            + YCSB_KEY
            + ", "
            + PAYLOAD
            + " from "
            + TABLE
            + " where "
            + YCSB_KEY
            + " in ("
            + placeholders
            + ")";

    Map<Integer, String> resultMap = new HashMap<>();
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int i = 0; i < userIds.size(); i++) {
        statement.setInt(i + 1, userIds.get(i));
      }
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        int key = resultSet.getInt(YCSB_KEY);
        String payload = resultSet.getString(PAYLOAD);
        resultMap.put(key, payload);
      }
    }

    List<String> results = new ArrayList<>(userIds.size());
    for (int userId : userIds) {
      results.add(resultMap.get(userId));
    }
    return results;
  }

  public static void write(Connection connection, List<Integer> userIds, List<String> payloads)
      throws SQLException {
    if (userIds.isEmpty()) {
      return;
    }

    String sql = "update " + TABLE + " set " + PAYLOAD + " = ? where " + YCSB_KEY + " = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int i = 0; i < userIds.size(); i++) {
        statement.setString(1, payloads.get(i));
        statement.setInt(2, userIds.get(i));
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }
}
