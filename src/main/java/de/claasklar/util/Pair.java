package de.claasklar.util;

import java.util.function.Function;

public record Pair<T, U>(T first, U second) {

  public <V> Pair<V, U> mapFirst(Function<T, V> mapper) {
    return new Pair<>(mapper.apply(first), second);
  }

  public <V> Pair<T, V> mapSecond(Function<U, V> mapper) {
    return new Pair<>(first, mapper.apply(second));
  }

  public <V> Pair<T, V> replaceSecond(V second) {
    return new Pair<>(first, second);
  }
}
