package kelpie.scalardb.transfer.graphql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import org.jetbrains.annotations.NotNull;

/** A {@code CookieJar} implementation that manages cookies for each thread. */
public class ThreadLocalCookieJar implements CookieJar {

  private static final ThreadLocal<List<Cookie>> cookies = ThreadLocal.withInitial(ArrayList::new);

  @NotNull
  @Override
  public List<Cookie> loadForRequest(@NotNull HttpUrl httpUrl) {
    List<Cookie> validCookies = new ArrayList<>();
    for (Iterator<Cookie> it = cookies.get().iterator(); it.hasNext(); ) {
      Cookie currentCookie = it.next();
      if (isCookieExpired(currentCookie)) {
        it.remove();
      } else if (currentCookie.matches(httpUrl)) {
        validCookies.add(currentCookie);
      }
    }
    return validCookies;
  }

  private static boolean isCookieExpired(Cookie cookie) {
    return cookie.expiresAt() < System.currentTimeMillis();
  }

  @Override
  public void saveFromResponse(@NotNull HttpUrl httpUrl, @NotNull List<Cookie> list) {
    cookies.get().addAll(list);
  }

  public void clearCookies() {
    cookies.get().clear();
  }
}
