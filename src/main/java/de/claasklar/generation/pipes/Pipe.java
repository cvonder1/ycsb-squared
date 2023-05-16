package de.claasklar.generation.pipes;

import java.util.function.Function;
import java.util.function.Supplier;

public interface Pipe<T, U> extends Function<T, U> {
  default Supplier<U> curry(T value) {
    return () -> apply(value);
  }

  default <V> Pipe<T, V> pipe(Pipe<U, V> next) {
    return (input) -> this.andThen(next).apply(input);
  }
}
