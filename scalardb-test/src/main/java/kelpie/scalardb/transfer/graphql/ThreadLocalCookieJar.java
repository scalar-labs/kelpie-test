package kelpie.scalardb.transfer.graphql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import org.jetbrains.annotations.NotNull;

/** A {@link CookieJar} implementation that manages cookies for each thread. */
public class ThreadLocalCookieJar implements CookieJar {

  private static final ThreadLocal<Set<IdentifiableCookie>> cookies =
      ThreadLocal.withInitial(HashSet::new);

  @NotNull
  @Override
  public List<Cookie> loadForRequest(@NotNull HttpUrl httpUrl) {
    List<Cookie> validCookies = new ArrayList<>();
    for (Iterator<IdentifiableCookie> it = cookies.get().iterator(); it.hasNext(); ) {
      Cookie currentCookie = it.next().cookie;
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
    list.stream().map(IdentifiableCookie::new).forEach(cookies.get()::add);
  }

  public void clearCookies() {
    cookies.get().clear();
  }

  /**
   * This class wraps a {@link Cookie} to re-implements equals() and hashcode() methods to identify
   * the cookie by the attributes: name, domain, path, secure, and hostOnly. By adding an instance
   * of this class to a {@link Set}, we can override the existing cookie with the same attributes.
   */
  private static class IdentifiableCookie {

    public final Cookie cookie;

    public IdentifiableCookie(Cookie cookie) {
      this.cookie = cookie;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IdentifiableCookie)) {
        return false;
      }
      IdentifiableCookie that = (IdentifiableCookie) o;
      return Objects.equals(that.cookie.name(), cookie.name())
          && Objects.equals(that.cookie.domain(), cookie.domain())
          && Objects.equals(that.cookie.path(), cookie.path())
          && Objects.equals(that.cookie.secure(), cookie.secure())
          && Objects.equals(that.cookie.hostOnly(), cookie.hostOnly());
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          cookie.name(), cookie.domain(), cookie.path(), cookie.secure(), cookie.hostOnly());
    }
  }
}
