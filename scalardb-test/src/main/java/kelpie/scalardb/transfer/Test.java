package kelpie.scalardb.transfer;

public class Test {
  public static void main(String[] args) {

    try {
      func();

    } catch (Exception e) {
      logTxWarn("error", e);
    }
  }

  private static void func() throws Exception {

    // try {
    throw new Exception("hello");

    /*
    } catch (Exception e) {
      throw new Exception("oh my god", e);
      //throw new Exception(e);
    }
       */
  }

  private static void logTxWarn(String message, Throwable e) {
    String cause = e.getMessage();
    if (e.getCause() != null) {
      cause = cause + " < " + e.getCause().getMessage();
    }
    System.out.println(message + ", cause: " + cause);
  }
}
