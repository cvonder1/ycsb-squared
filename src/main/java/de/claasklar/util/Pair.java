package de.claasklar.util;

import java.util.function.Function;

public record Pair<T, U>(T first, U second) {
  public <V> Pair<T, V> mapSecond(Function<U, V> mapper) {
    return new Pair(first, mapper.apply(second));
  }
}