package kelpie.scalardb.ycsb.sql;

import com.scalar.kelpie.config.Config;
import java.util.Random;

public class YcsbCommon {
  static final long DEFAULT_LOAD_CONCURRENCY = 1;
  static final long DEFAULT_LOAD_BATCH_SIZE = 1;
  static final long DEFAULT_TABLE_COUNT = 3;
  static final long DEFAULT_RECORD_COUNT = 1000;
  static final long DEFAULT_PAYLOAD_SIZE = 1000;
  static final String NAMESPACE = "ycsb";
  static final String TABLE = "usertable";
  static final String YCSB_KEY = "ycsb_key";
  static final String PAYLOAD = "payload";
  static final String CONFIG_NAME = "ycsb_config";
  static final String LOAD_CONCURRENCY = "load_concurrency";
  static final String LOAD_BATCH_SIZE = "load_batch_size";
  static final String LOAD_OVERWRITE = "load_overwrite";
  static final String TABLE_COUNT = "table_count";
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

  public static int getLoadConcurrency(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, LOAD_CONCURRENCY, DEFAULT_LOAD_CONCURRENCY);
  }

  public static long getLoadBatchSize(Config config) {
    return config.getUserLong(CONFIG_NAME, LOAD_BATCH_SIZE, DEFAULT_LOAD_BATCH_SIZE);
  }

  public static boolean getLoadOverwrite(Config config) {
    return config.getUserBoolean(CONFIG_NAME, LOAD_OVERWRITE, false);
  }

  public static int getTableCount(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, TABLE_COUNT, DEFAULT_TABLE_COUNT);
  }

  public static long getRecordCount(Config config) {
    return config.getUserLong(CONFIG_NAME, RECORD_COUNT, DEFAULT_RECORD_COUNT);
  }

  public static int getPayloadSize(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, PAYLOAD_SIZE, DEFAULT_PAYLOAD_SIZE);
  }

  public static String getTableName(long userId, int tableCount) {
    long namespaceIndex = userId % tableCount;
    return YcsbCommon.NAMESPACE + namespaceIndex + "." + TABLE;
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

  public static String getConfigFile(Config config) {
    try {
      Class.forName("com.scalar.db.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return config.getUserString(CONFIG_NAME, "config_file");
  }
}
