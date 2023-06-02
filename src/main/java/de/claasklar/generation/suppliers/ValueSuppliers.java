package de.claasklar.generation.suppliers;

import de.claasklar.primitives.document.ArrayValue;
import de.claasklar.primitives.document.DoubleValue;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.StringValue;
import de.claasklar.random.distribution.RandomNumberGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ValueSuppliers {

  private final RandomNumberGenerator random;

  public ValueSuppliers(RandomNumberGenerator randomNumberGenerator) {
    this.random = randomNumberGenerator;
  }

  public ValueSupplier uniformIntSupplier(int lower, int upper) {
    return () -> new DoubleValue(random.nextInt(lower, upper));
  }

  public ValueSupplier uniformLengthStringSupplier(int lower, int upper) {
    return () -> {
      var length = random.nextInt(lower, upper);
      var sb = new StringBuilder(length);
      for (int i = 0; i < length; i++) {
        sb.append((char) random.nextLong(0, Character.MAX_VALUE));
      }
      return new StringValue(sb.toString());
    };
  }

  public ValueSupplier repeat(String key, Supplier<Integer> times, ValueSupplier singleSupplier) {
    return () -> {
      var length = times.get();
      var array = new ArrayValue();
      for (int i = 0; i < length; i++) {
        array.add(singleSupplier.get());
      }
      return array;
    };
  }

  public ValueSupplier objectSupplier(Consumer<Map<String, ValueSupplier>> config) {
    Map<String, ValueSupplier> map = new HashMap<>();
    config.accept(map);
    return this.objectSupplier(map);
  }

  public ValueSupplier objectSupplier(Map<String, ValueSupplier> fields) {
    return () -> {
      var object = new NestedObjectValue();
      fields.entrySet().stream()
          .forEach(entry -> object.put(entry.getKey(), entry.getValue().get()));
      return object;
    };
  }
}
