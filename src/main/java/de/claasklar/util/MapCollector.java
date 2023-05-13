package de.claasklar.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class MapCollector<T, U> implements Collector<Pair<T, U>, Map<T, U>, Map<T, U>> {

  @Override
  public Supplier<Map<T, U>> supplier() {
    return HashMap::new;
  }

  @Override
  public BiConsumer<Map<T, U>, Pair<T, U>> accumulator() {
    return (acc, pair) -> {
      acc.put(pair.first(), pair.second());
    };
  }

  @Override
  public BinaryOperator<Map<T, U>> combiner() {
    return (first, second) -> {
      first.putAll(second);
      return first;
    };
  }

  @Override
  public Function<Map<T, U>, Map<T, U>> finisher() {
    return it -> it;
  }

  @Override
  public Set<Characteristics> characteristics() {
    return Set.of(Characteristics.UNORDERED, Characteristics.IDENTITY_FINISH);
  }
}
